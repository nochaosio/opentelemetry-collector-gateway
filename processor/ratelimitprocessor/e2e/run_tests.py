#!/usr/bin/env python3
"""
Rate Limit Processor — E2E Test Runner

Reads scenarios.yaml, starts the collector gateway, runs telemetrygen for each
scenario, and validates the Prometheus metrics to confirm rate limiting behaviour.

Usage:
    cd processor/ratelimitprocessor/e2e
    source .venv/bin/activate
    python3 run_tests.py [--scenarios scenarios.yaml] [--results-dir results/]
"""

import argparse
import os
import re
import subprocess
import sys
import time
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

try:
    import yaml
except ImportError:
    print("Error: pyyaml not found. Install with: pip install pyyaml")
    sys.exit(1)

# ─── Terminal colours ──────────────────────────────────────────────────────────

RESET = "\033[0m"
GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
BOLD = "\033[1m"


def green(s: str) -> str:
    return f"{GREEN}{s}{RESET}"


def red(s: str) -> str:
    return f"{RED}{s}{RESET}"


def yellow(s: str) -> str:
    return f"{YELLOW}{s}{RESET}"


def cyan(s: str) -> str:
    return f"{CYAN}{s}{RESET}"


def bold(s: str) -> str:
    return f"{BOLD}{s}{RESET}"


# ─── Data structures ───────────────────────────────────────────────────────────

@dataclass
class ScenarioResult:
    id: str
    name: str
    description: str
    status: str           # PASS | FAIL | ERROR
    allowed: int = 0
    denied: int = 0
    received: int = 0
    expected_allowed_min: int = 0
    expected_allowed_max: int = 0
    expected_denied_min: int = 0
    expected_denied_max: int = -1  # -1 = no upper bound
    error: str = ""
    telemetrygen_cmd: str = ""


# ─── Metrics helpers ───────────────────────────────────────────────────────────

def fetch_metrics(address: str) -> str:
    """Fetch the raw Prometheus text from the collector metrics endpoint."""
    try:
        with urllib.request.urlopen(address, timeout=5) as resp:
            return resp.read().decode("utf-8")
    except Exception:
        return ""


def parse_metric(text: str, metric_pattern: str, key: str, signal: str) -> int:
    """
    Extract a Prometheus counter value filtered by key and signal labels.

    Handles metric names with or without the otelcol_ prefix and with or
    without the _total suffix. Also handles both label ordering variants.
    """
    key_escaped = re.escape(key)
    signal_escaped = re.escape(signal)

    # key label before signal label
    pattern = (
        rf"(?:otelcol_)?{re.escape(metric_pattern)}(?:_total)?"
        rf"\{{[^}}]*key=\"{key_escaped}\"[^}}]*signal=\"{signal_escaped}\"[^}}]*\}}"
        rf"\s+([\d.]+)"
    )
    matches = re.findall(pattern, text)
    if matches:
        return int(float(matches[-1]))

    # signal label before key label (fallback)
    pattern2 = (
        rf"(?:otelcol_)?{re.escape(metric_pattern)}(?:_total)?"
        rf"\{{[^}}]*signal=\"{signal_escaped}\"[^}}]*key=\"{key_escaped}\"[^}}]*\}}"
        rf"\s+([\d.]+)"
    )
    matches2 = re.findall(pattern2, text)
    if matches2:
        return int(float(matches2[-1]))

    return 0


def read_counts(address: str, key: str, signal: str) -> tuple[int, int, int]:
    """Return (received, allowed, denied) for a given key + signal pair."""
    text = fetch_metrics(address)
    received = parse_metric(text, "otelcol_processor_ratelimit_received_items", key, signal)
    allowed  = parse_metric(text, "otelcol_processor_ratelimit_allowed_items",  key, signal)
    denied   = parse_metric(text, "otelcol_processor_ratelimit_denied_items",   key, signal)
    return received, allowed, denied


# ─── Prerequisites ─────────────────────────────────────────────────────────────

def check_binary(binary_path: str) -> bool:
    path = Path(binary_path)
    if not path.exists():
        print(red(f"  ✗ Binary not found: {path.resolve()}"))
        print(yellow("    Run 'make build' from the project root first."))
        return False
    if not os.access(path, os.X_OK):
        print(red(f"  ✗ Binary is not executable: {path.resolve()}"))
        return False
    return True


def check_telemetrygen() -> bool:
    result = subprocess.run(
        ["which", "telemetrygen"], capture_output=True, text=True
    )
    if result.returncode != 0:
        print(red("  ✗ telemetrygen not found in PATH"))
        print(yellow(
            "    Install with:\n"
            "    go install github.com/open-telemetry/opentelemetry-collector-contrib"
            "/cmd/telemetrygen@latest"
        ))
        return False
    return True


def check_prerequisites(binary_path: str) -> bool:
    print(bold("\nChecking prerequisites..."))
    ok = True
    ok = check_binary(binary_path) and ok
    ok = check_telemetrygen() and ok
    return ok


# ─── Collector lifecycle ───────────────────────────────────────────────────────

def start_collector(binary: str, config: str, metrics_address: str,
                    startup_wait: int) -> Optional[subprocess.Popen]:
    print(bold(f"\nStarting collector: {binary}"))
    print(f"  Config: {config}")

    proc = subprocess.Popen(
        [binary, "--config", config],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    print(f"  Waiting {startup_wait}s for collector to start...", end="", flush=True)
    time.sleep(startup_wait)

    if proc.poll() is not None:
        print(red(" FAILED"))
        print(red(f"  Collector exited with code {proc.returncode}"))
        return None

    # Verify the metrics endpoint is responding
    for _ in range(10):
        text = fetch_metrics(metrics_address)
        if text:
            print(green(" OK"))
            return proc
        time.sleep(0.5)

    print(red(" FAILED"))
    print(red(f"  Metrics endpoint did not respond: {metrics_address}"))
    proc.terminate()
    return None


def stop_collector(proc: subprocess.Popen):
    if proc and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
    print(bold("\nCollector stopped."))


# ─── Scenario execution ────────────────────────────────────────────────────────

def build_telemetrygen_cmd(scenario: dict) -> list[str]:
    """Build the telemetrygen command from a scenario definition."""
    tg = scenario["telemetrygen"]
    signal = scenario["signal"]
    port = scenario["port"]

    cmd = [
        "telemetrygen",
        signal,
        "--otlp-endpoint", f"localhost:{port}",
        "--otlp-insecure",
        "--rate", str(tg["rate"]),
        "--duration", tg["duration"],
        "--workers", str(tg.get("workers", 1)),
        # Continue sending even when the server returns an error (rate limit drop)
        "--allow-export-failures",
    ]

    if "service_name" in tg:
        cmd += ["--service", tg["service_name"]]

    # Custom resource attributes (requires telemetrygen with --otlp-attributes support)
    if "resource_attributes" in tg:
        for k, v in tg["resource_attributes"].items():
            cmd += ["--otlp-attributes", f'{k}="{v}"']

    return cmd


def run_scenario(scenario: dict, metrics_address: str,
                 settle_seconds: int) -> ScenarioResult:
    sid    = scenario["id"]
    name   = scenario["name"]
    desc   = scenario.get("description", "")
    signal = scenario["signal"]
    key    = scenario["metric_key"]
    exp    = scenario["expected"]

    result = ScenarioResult(
        id=sid,
        name=name,
        description=desc,
        status="ERROR",
        expected_allowed_min=exp.get("allowed_min", 0),
        expected_allowed_max=exp.get("allowed_max", 10_000),
        expected_denied_min=exp.get("denied_min", 0),
        expected_denied_max=exp.get("denied_max", -1),
    )

    cmd = build_telemetrygen_cmd(scenario)
    result.telemetrygen_cmd = " ".join(cmd)

    # Snapshot counters before sending to compute a clean delta
    before_recv, before_allowed, before_denied = read_counts(metrics_address, key, signal)

    # Run telemetrygen
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except subprocess.TimeoutExpired:
        result.error = "telemetrygen timed out after 30s"
        return result
    except FileNotFoundError:
        result.error = "telemetrygen not found"
        return result

    if proc.returncode not in (0, 1):
        # Exit code 1 is expected when the server rejects requests (rate limit drop);
        # other non-zero codes indicate a real error
        result.error = (
            f"telemetrygen exited with code {proc.returncode}: {proc.stderr[:200]}"
        )
        return result

    # Wait for metrics to propagate
    time.sleep(settle_seconds)

    # Read new counters and compute delta
    after_recv, after_allowed, after_denied = read_counts(metrics_address, key, signal)

    result.received = after_recv    - before_recv
    result.allowed  = after_allowed - before_allowed
    result.denied   = after_denied  - before_denied

    # Validate against expected ranges
    allowed_ok = result.expected_allowed_min <= result.allowed <= result.expected_allowed_max
    denied_ok  = result.denied >= result.expected_denied_min
    if result.expected_denied_max >= 0:
        denied_ok = denied_ok and result.denied <= result.expected_denied_max

    if allowed_ok and denied_ok:
        result.status = "PASS"
    else:
        result.status = "FAIL"
        reasons = []
        if not allowed_ok:
            reasons.append(
                f"allowed={result.allowed} outside range "
                f"[{result.expected_allowed_min}, {result.expected_allowed_max}]"
            )
        if not denied_ok:
            denied_range = f">= {result.expected_denied_min}"
            if result.expected_denied_max >= 0:
                denied_range = (
                    f"[{result.expected_denied_min}, {result.expected_denied_max}]"
                )
            reasons.append(f"denied={result.denied} outside {denied_range}")
        result.error = "; ".join(reasons)

    return result


# ─── Reporting ─────────────────────────────────────────────────────────────────

def print_scenario_header(idx: int, total: int, scenario: dict):
    sid  = scenario["id"]
    name = scenario["name"]
    port = scenario["port"]
    print(f"\n{CYAN}[{idx}/{total}]{RESET} {bold(name)}")
    print(f"  ID: {sid} | Port: {port}")
    print(f"  {scenario.get('description', '').strip()}")


def print_scenario_result(result: ScenarioResult):
    if result.status == "PASS":
        icon = green("✓ PASS")
    elif result.status == "FAIL":
        icon = red("✗ FAIL")
    else:
        icon = yellow(f"⚠ {result.status}")

    print(
        f"  {icon} | "
        f"received={result.received} allowed={result.allowed} denied={result.denied}"
    )
    if result.error:
        print(f"  {yellow('Detail:')} {result.error}")


def generate_markdown_report(results: list[ScenarioResult], scenarios_file: str,
                              binary: str, started_at: str) -> str:
    passed  = sum(1 for r in results if r.status == "PASS")
    failed  = sum(1 for r in results if r.status == "FAIL")
    errored = sum(1 for r in results if r.status == "ERROR")
    total   = len(results)

    summary_icon = "✅" if failed == 0 and errored == 0 else "❌"

    lines = [
        "# Rate Limit Processor — E2E Test Report",
        "",
        f"**Generated:** {started_at}  ",
        f"**Binary:** `{binary}`  ",
        f"**Scenarios:** `{scenarios_file}`  ",
        "",
        f"## Summary {summary_icon}",
        "",
        "| Total | Passed | Failed | Errors |",
        "|-------|--------|--------|--------|",
        f"| {total} | {passed} | {failed} | {errored} |",
        "",
        "## Results",
        "",
        "| # | ID | Name | Received | Allowed | Denied | Status |",
        "|---|----|------|----------|---------|--------|--------|",
    ]

    for i, r in enumerate(results, 1):
        status_md = "✅ PASS" if r.status == "PASS" else f"❌ {r.status}"
        lines.append(
            f"| {i} | `{r.id}` | {r.name} "
            f"| {r.received} | {r.allowed} | {r.denied} | {status_md} |"
        )

    lines += ["", "---", "", "## Scenario Details", ""]

    for r in results:
        status_md = "✅ PASS" if r.status == "PASS" else f"❌ {r.status}"
        lines += [
            f"### {r.name}",
            "",
            f"**ID:** `{r.id}`  ",
            f"**Status:** {status_md}  ",
            "",
            f"> {r.description}",
            "",
            "**Command:**",
            "```bash",
            r.telemetrygen_cmd,
            "```",
            "",
            "**Expected:**",
            f"- allowed: [{r.expected_allowed_min}, {r.expected_allowed_max}]",
        ]
        denied_range = f">= {r.expected_denied_min}"
        if r.expected_denied_max >= 0:
            denied_range = (
                f"[{r.expected_denied_min}, {r.expected_denied_max}]"
            )
        lines.append(f"- denied: {denied_range}")
        lines += [
            "",
            "**Got:**",
            f"- received: {r.received}",
            f"- allowed:  {r.allowed}",
            f"- denied:   {r.denied}",
        ]
        if r.error:
            lines += ["", f"**Detail:** {r.error}"]
        lines.append("")

    return "\n".join(lines)


# ─── Entry point ───────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="E2E test runner for ratelimitprocessor using telemetrygen"
    )
    parser.add_argument(
        "--scenarios",
        default="scenarios.yaml",
        help="Path to the scenarios file (default: scenarios.yaml)",
    )
    parser.add_argument(
        "--results-dir",
        default="results",
        help="Directory to save the Markdown report (default: results/)",
    )
    parser.add_argument(
        "--scenario",
        metavar="ID",
        help="Run only the scenario with this ID (e.g. svc_rps_drop)",
    )
    parser.add_argument(
        "--no-report",
        action="store_true",
        help="Skip saving the Markdown report to disk",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    script_dir  = Path(__file__).parent.resolve()
    started_at  = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    # Load scenarios
    scenarios_path = script_dir / args.scenarios
    if not scenarios_path.exists():
        print(red(f"Scenarios file not found: {scenarios_path}"))
        sys.exit(1)

    with open(scenarios_path) as f:
        config = yaml.safe_load(f)

    settings      = config["settings"]
    all_scenarios = config["scenarios"]

    # Filter to a single scenario if requested
    if args.scenario:
        all_scenarios = [s for s in all_scenarios if s["id"] == args.scenario]
        if not all_scenarios:
            print(red(f"Scenario '{args.scenario}' not found in {scenarios_path}"))
            sys.exit(1)

    binary           = str((script_dir / settings["binary"]).resolve())
    collector_config = str((script_dir / settings["config"]).resolve())
    metrics_address  = settings["metrics_address"]
    startup_wait     = settings.get("startup_wait_seconds", 3)
    settle_seconds   = settings.get("scenario_settle_seconds", 1)

    print(bold("\n" + "=" * 60))
    print(bold("  Rate Limit Processor — E2E Tests"))
    print(bold("=" * 60))
    print(f"  Scenarios: {len(all_scenarios)}")
    print(f"  Binary:    {binary}")

    if not check_prerequisites(binary):
        sys.exit(1)

    collector_proc = start_collector(
        binary, collector_config, metrics_address, startup_wait
    )
    if collector_proc is None:
        sys.exit(1)

    results: list[ScenarioResult] = []

    try:
        total = len(all_scenarios)
        for idx, scenario in enumerate(all_scenarios, 1):
            print_scenario_header(idx, total, scenario)
            result = run_scenario(scenario, metrics_address, settle_seconds)
            results.append(result)
            print_scenario_result(result)
    finally:
        stop_collector(collector_proc)

    # Final summary
    passed = sum(1 for r in results if r.status == "PASS")
    failed = sum(1 for r in results if r.status != "PASS")
    total  = len(results)

    print(bold("\n" + "=" * 60))
    print(bold("  Summary"))
    print(bold("=" * 60))
    print(f"  Total:   {total}")
    print(f"  {green(f'Passed:  {passed}')}")
    if failed:
        print(f"  {red(f'Failed:  {failed}')}")

    # Save Markdown report
    if not args.no_report:
        results_dir = script_dir / args.results_dir
        results_dir.mkdir(exist_ok=True)
        timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = results_dir / f"report_{timestamp}.md"
        report_md   = generate_markdown_report(
            results, str(scenarios_path), binary, started_at
        )
        report_path.write_text(report_md)
        print(f"\n  Report saved to: {cyan(str(report_path))}")

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
