package statefulfilterprocessor

import (
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

func mustMap(kv map[string]string) pcommon.Map {
	m := pcommon.NewMap()
	for k, v := range kv {
		m.PutStr(k, v)
	}
	return m
}

// compileOne is a test helper: compile a single rule document and fail on error.
func compileOne(t *testing.T, doc string) *compiledRule {
	t.Helper()
	cr, _, err := compile([]byte(doc), "test-rule")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if cr == nil {
		t.Fatal("compile returned nil rule (disabled?)")
	}
	return cr
}

func TestCompile_RejectsAmbiguousRules(t *testing.T) {
	cases := map[string]string{
		"empty conditions":  `{"id":"a","conditions":[]}`,
		"no conditions key": `{"id":"a"}`,
		"unknown source":    `{"id":"a","conditions":[{"source":"planet","value":"x"}]}`,
		"unknown op":        `{"id":"a","conditions":[{"source":"name","op":"sounds_like","value":"x"}]}`,
		"unknown action":    `{"id":"a","action":"maybe","conditions":[{"source":"name","value":"x"}]}`,
		"unknown signal":    `{"id":"a","signals":["profiles"],"conditions":[{"source":"name","value":"x"}]}`,
		"missing key":       `{"id":"a","conditions":[{"source":"attribute","value":"x"}]}`,
		"bad regex":         `{"id":"a","conditions":[{"source":"name","op":"regex","value":"["}]}`,
		"equals no value":   `{"id":"a","conditions":[{"source":"name","op":"equals"}]}`,
		"bad expires_at":    `{"id":"a","expires_at":"tomorrow","conditions":[{"source":"name","value":"x"}]}`,
		"invalid json":      `{"id":"a",`,
	}
	for name, doc := range cases {
		t.Run(name, func(t *testing.T) {
			if _, _, err := compile([]byte(doc), "a"); err == nil {
				t.Fatalf("expected compile error for %s", name)
			}
		})
	}
}

func TestCompile_DisabledRuleIsSkippedNotFailed(t *testing.T) {
	cr, _, err := compile([]byte(`{"id":"a","enabled":false,"conditions":[{"source":"name","value":"x"}]}`), "a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cr != nil {
		t.Fatal("disabled rule should not compile into an active rule")
	}
}

func TestCompile_DefaultsToDropOnAllSignals(t *testing.T) {
	cr, signals, err := compile([]byte(`{"id":"a","conditions":[{"source":"name","value":"x"}]}`), "a")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if cr.action != actionDrop {
		t.Fatalf("expected default action drop, got %q", cr.action)
	}
	if len(signals) != 3 {
		t.Fatalf("expected all 3 signals by default, got %v", signals)
	}
}

func TestCompile_IDFallsBackToHashField(t *testing.T) {
	cr, _, err := compile([]byte(`{"conditions":[{"source":"name","value":"x"}]}`), "field-name")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if cr.id != "field-name" {
		t.Fatalf("expected id from hash field, got %q", cr.id)
	}
}

func TestConditionOperators(t *testing.T) {
	e := &evalCtx{
		resource: mustMap(map[string]string{"service.name": "Checkout"}),
		attrs:    mustMap(map[string]string{"http.route": "/healthz/ready", "env": "prod"}),
		name:     "GET /healthz",
		scope:    "otelhttp",
		body:     "connection reset by peer",
		severity: "INFO",
	}
	now := time.Now()

	cases := []struct {
		name string
		doc  string
		want bool
	}{
		{"equals hit", `{"conditions":[{"source":"attribute","key":"env","value":"prod"}]}`, true},
		{"equals miss", `{"conditions":[{"source":"attribute","key":"env","value":"dev"}]}`, false},
		{"values any-of", `{"conditions":[{"source":"attribute","key":"env","values":["dev","prod"]}]}`, true},
		{"not_equals hit", `{"conditions":[{"source":"attribute","key":"env","op":"not_equals","value":"dev"}]}`, true},
		{"not_equals on absent key does not match", `{"conditions":[{"source":"attribute","key":"nope","op":"not_equals","value":"dev"}]}`, false},
		{"exists", `{"conditions":[{"source":"attribute","key":"env","op":"exists"}]}`, true},
		{"not_exists", `{"conditions":[{"source":"attribute","key":"nope","op":"not_exists"}]}`, true},
		{"prefix", `{"conditions":[{"source":"attribute","key":"http.route","op":"prefix","value":"/healthz"}]}`, true},
		{"suffix", `{"conditions":[{"source":"attribute","key":"http.route","op":"suffix","value":"ready"}]}`, true},
		{"contains", `{"conditions":[{"source":"body","op":"contains","value":"reset by"}]}`, true},
		{"regex", `{"conditions":[{"source":"name","op":"regex","value":"^GET /health.*"}]}`, true},
		{"ignore_case", `{"conditions":[{"source":"resource","key":"service.name","value":"checkout","ignore_case":true}]}`, true},
		{"case sensitive by default", `{"conditions":[{"source":"resource","key":"service.name","value":"checkout"}]}`, false},
		{"scope", `{"conditions":[{"source":"scope","value":"otelhttp"}]}`, true},
		{"severity", `{"conditions":[{"source":"severity","value":"INFO"}]}`, true},
		{"AND: both hold", `{"conditions":[{"source":"attribute","key":"env","value":"prod"},{"source":"name","op":"prefix","value":"GET"}]}`, true},
		{"AND: one fails", `{"conditions":[{"source":"attribute","key":"env","value":"prod"},{"source":"name","op":"prefix","value":"POST"}]}`, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cr := compileOne(t, tc.doc)
			if got := cr.matches(e, now); got != tc.want {
				t.Fatalf("matches = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestRule_Expiry(t *testing.T) {
	e := &evalCtx{attrs: mustMap(map[string]string{"env": "prod"})}
	cr := compileOne(t, `{"id":"a","expires_at":"2020-01-01T00:00:00Z","conditions":[{"source":"attribute","key":"env","value":"prod"}]}`)

	if cr.matches(e, time.Now()) {
		t.Fatal("expired rule must not match")
	}
	before, _ := time.Parse(time.RFC3339, "2019-06-01T00:00:00Z")
	if !cr.matches(e, before) {
		t.Fatal("rule must match before its expiry")
	}
}

func TestBuildRuleSet_KeepRulesEvaluatedFirst(t *testing.T) {
	docs := map[string]string{
		// Sorted-id order puts "a-drop" first; the keep rule must still win.
		"a-drop": `{"action":"drop","signals":["traces"],"conditions":[{"source":"resource","key":"service.name","value":"noisy"}]}`,
		"z-keep": `{"action":"keep","signals":["traces"],"conditions":[{"source":"attribute","key":"debug","op":"exists"}]}`,
	}
	rs := buildRuleSet(docs, 7, defaultMaxRules, nil)

	if rs.total != 2 || rs.invalid != 0 {
		t.Fatalf("expected 2 valid rules, got total=%d invalid=%d", rs.total, rs.invalid)
	}
	if rs.traces[0].action != actionKeep {
		t.Fatalf("keep rule must sort first, got %q", rs.traces[0].id)
	}

	e := &evalCtx{
		resource: mustMap(map[string]string{"service.name": "noisy"}),
		attrs:    mustMap(map[string]string{"debug": "1"}),
	}
	if r := rs.match(signalTraces, e, time.Now()); r == nil || r.action != actionKeep {
		t.Fatalf("expected the keep rule to claim the item, got %+v", r)
	}
	if _, drop := rs.shouldDrop(signalTraces, e, time.Now()); drop {
		t.Fatal("keep rule must veto the drop")
	}
}

func TestBuildRuleSet_InvalidRuleDoesNotBlankTheSet(t *testing.T) {
	var reported []string
	docs := map[string]string{
		"good": `{"conditions":[{"source":"name","value":"x"}]}`,
		"bad":  `{"conditions":[{"source":"nonsense","value":"x"}]}`,
	}
	rs := buildRuleSet(docs, 1, defaultMaxRules, func(id string, _ error) {
		reported = append(reported, id)
	})

	if rs.total != 1 {
		t.Fatalf("expected the good rule to survive, total=%d", rs.total)
	}
	if rs.invalid != 1 {
		t.Fatalf("expected invalid=1, got %d", rs.invalid)
	}
	if len(reported) != 1 || reported[0] != "bad" {
		t.Fatalf("expected the bad rule to be reported, got %v", reported)
	}
}

func TestBuildRuleSet_SignalScoping(t *testing.T) {
	docs := map[string]string{
		"logs-only": `{"signals":["logs"],"conditions":[{"source":"severity","value":"DEBUG"}]}`,
	}
	rs := buildRuleSet(docs, 1, defaultMaxRules, nil)

	if len(rs.logs) != 1 {
		t.Fatalf("expected 1 log rule, got %d", len(rs.logs))
	}
	if len(rs.traces) != 0 || len(rs.metrics) != 0 {
		t.Fatalf("rule must not leak into other signals: traces=%d metrics=%d", len(rs.traces), len(rs.metrics))
	}
}

func TestBuildRuleSet_MaxRulesTruncatesDeterministically(t *testing.T) {
	docs := map[string]string{
		"c": `{"conditions":[{"source":"name","value":"c"}]}`,
		"a": `{"conditions":[{"source":"name","value":"a"}]}`,
		"b": `{"conditions":[{"source":"name","value":"b"}]}`,
	}
	// Every replica must truncate the same way, otherwise the same span is
	// dropped on one collector and kept on another.
	first := buildRuleSet(docs, 1, 2, nil)
	second := buildRuleSet(docs, 1, 2, nil)

	if first.total != 2 || second.total != 2 {
		t.Fatalf("expected 2 rules loaded, got %d and %d", first.total, second.total)
	}
	if first.traces[0].id != "a" || first.traces[1].id != "b" {
		t.Fatalf("expected sorted truncation [a b], got [%s %s]", first.traces[0].id, first.traces[1].id)
	}
	if second.traces[0].id != first.traces[0].id || second.traces[1].id != first.traces[1].id {
		t.Fatal("truncation must be deterministic across replicas")
	}
	if first.invalid != 1 {
		t.Fatalf("expected the dropped rule to be reported as invalid, got %d", first.invalid)
	}
}

func TestBuildRuleSet_NeedsBodyOnlyWhenARuleReadsIt(t *testing.T) {
	without := buildRuleSet(map[string]string{
		"a": `{"signals":["logs"],"conditions":[{"source":"severity","value":"DEBUG"}]}`,
	}, 1, defaultMaxRules, nil)
	if without.needsBody {
		t.Fatal("needsBody must stay false when no rule reads the body")
	}

	with := buildRuleSet(map[string]string{
		"a": `{"signals":["logs"],"conditions":[{"source":"body","op":"contains","value":"ping"}]}`,
	}, 1, defaultMaxRules, nil)
	if !with.needsBody {
		t.Fatal("needsBody must be set when a log rule reads the body")
	}
}
