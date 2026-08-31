// Copyright The NOCHAOS Authors
// SPDX-License-Identifier: Apache-2.0

package statefulfilterprocessor

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// Signal names used both in the rule JSON (`signals`) and as the metric label.
const (
	signalTraces  = "traces"
	signalMetrics = "metrics"
	signalLogs    = "logs"
)

// Rule actions.
const (
	actionDrop = "drop"
	actionKeep = "keep"
)

// Condition sources.
const (
	sourceResource  = "resource"  // resource attribute (service.name, k8s.*, ...)
	sourceAttribute = "attribute" // span / log record / metric datapoint attribute
	sourceName      = "name"      // span name, metric name (no meaning for logs)
	sourceBody      = "body"      // log record body, stringified (logs only)
	sourceSeverity  = "severity"  // log severity text (logs only)
	sourceScope     = "scope"     // instrumentation scope name
)

// Condition operators.
const (
	opEquals    = "equals"
	opNotEquals = "not_equals"
	opContains  = "contains"
	opPrefix    = "prefix"
	opSuffix    = "suffix"
	opRegex     = "regex"
	opExists    = "exists"
	opNotExists = "not_exists"
)

// Rule is the JSON document stored in one field of the Redis rule hash. It is
// deliberately declarative (no expression language): the writer is often an
// on-call human with redis-cli at 3am, or a control plane that has no business
// generating OTTL.
//
//	{
//	  "id": "drop-healthz",
//	  "description": "kube probes flooding the trace backend",
//	  "action": "drop",
//	  "signals": ["traces"],
//	  "conditions": [
//	    {"source": "resource",  "key": "service.name", "op": "equals",   "value": "checkout"},
//	    {"source": "attribute", "key": "http.route",   "op": "prefix",   "value": "/healthz"}
//	  ],
//	  "expires_at": "2026-08-06T00:00:00Z"
//	}
type Rule struct {
	ID          string `json:"id"`
	Description string `json:"description,omitempty"`

	// Enabled defaults to true when absent, so a minimal rule document works.
	// Setting it false is the "turn this off but keep it around" switch — the
	// reason it is a pointer rather than a plain bool.
	Enabled *bool `json:"enabled,omitempty"`

	// Action: "drop" (default) or "keep". Keep rules are exceptions: they are
	// evaluated first and short-circuit every drop rule for that item.
	Action string `json:"action,omitempty"`

	// Signals restricts the rule to a subset of traces/metrics/logs. Empty
	// means all three.
	Signals []string `json:"signals,omitempty"`

	// Conditions are ANDed. An empty condition list is rejected — a rule that
	// matches everything is almost never what someone meant to type, and as a
	// drop rule it is an outage.
	Conditions []Condition `json:"conditions"`

	// ExpiresAt (RFC3339) auto-disables the rule. Incident-response drops
	// ("mute this noisy service for an hour") should not outlive the incident;
	// without this they become permanent by accident.
	ExpiresAt string `json:"expires_at,omitempty"`
}

// Condition is a single predicate over one field of the item being evaluated.
type Condition struct {
	Source string `json:"source"`
	Key    string `json:"key,omitempty"` // required for resource/attribute
	Op     string `json:"op,omitempty"`  // default "equals"
	Value  string `json:"value,omitempty"`

	// Values is an any-of list for equals/not_equals — "drop these 12 noisy
	// services" without writing 12 rules.
	Values []string `json:"values,omitempty"`

	// IgnoreCase applies to equals/not_equals/contains/prefix/suffix. For regex
	// use an inline (?i) flag instead.
	IgnoreCase bool `json:"ignore_case,omitempty"`
}

// compiledRule is the hot-path form: regexes pre-compiled, case folding
// pre-applied, expiry pre-parsed. Never mutated after construction, so a
// ruleSet can be shared by every pipeline goroutine without locking.
type compiledRule struct {
	id        string
	action    string
	conds     []compiledCond
	expiresAt time.Time // zero == never expires
}

type compiledCond struct {
	source     string
	key        string
	op         string
	value      string   // already lower-cased when ignoreCase
	values     []string // ditto
	re         *regexp.Regexp
	ignoreCase bool
}

// ruleSet is an immutable snapshot of the Redis rule hash, published to the
// data path via a single atomic pointer swap. Readers never lock and never see
// a half-applied update.
type ruleSet struct {
	version int64
	loadedA time.Time

	// Per-signal lists, keep-rules first so matching can short-circuit on the
	// exception before paying for any drop rule.
	traces  []*compiledRule
	metrics []*compiledRule
	logs    []*compiledRule

	// total is the number of rules that compiled successfully; invalid counts
	// the documents that were rejected (bad JSON, unknown op, bad regex) and is
	// exported as a metric — a rule silently not applying is the failure mode
	// operators most need to see.
	total   int
	invalid int

	// needsBody is set when any log rule reads the record body, so the hot path
	// can skip stringifying bodies nobody looks at.
	needsBody bool
}

// emptyRuleSet is the pass-through snapshot used before the first successful
// load. Distinguished from nil so the data path never nil-checks.
func emptyRuleSet() *ruleSet { return &ruleSet{} }

func (rs *ruleSet) rulesFor(signal string) []*compiledRule {
	switch signal {
	case signalTraces:
		return rs.traces
	case signalMetrics:
		return rs.metrics
	case signalLogs:
		return rs.logs
	}
	return nil
}

// evalCtx is the view of one telemetry item (span, log record, metric
// datapoint) that conditions are evaluated against. Fields not applicable to
// the current signal stay zero and their conditions simply never match.
type evalCtx struct {
	resource pcommon.Map
	attrs    pcommon.Map
	scope    string
	name     string
	body     string
	severity string
}

// lookup resolves a condition's source to (value, present). Absence matters:
// it is what separates `not_equals` (present and different) from `not_exists`.
func (e *evalCtx) lookup(c *compiledCond) (string, bool) {
	switch c.source {
	case sourceResource:
		if v, ok := e.resource.Get(c.key); ok {
			return v.AsString(), true
		}
		return "", false
	case sourceAttribute:
		if v, ok := e.attrs.Get(c.key); ok {
			return v.AsString(), true
		}
		return "", false
	case sourceName:
		return e.name, e.name != ""
	case sourceBody:
		return e.body, e.body != ""
	case sourceSeverity:
		return e.severity, e.severity != ""
	case sourceScope:
		return e.scope, e.scope != ""
	}
	return "", false
}

func (c *compiledCond) eval(e *evalCtx) bool {
	got, present := e.lookup(c)

	switch c.op {
	case opExists:
		return present
	case opNotExists:
		return !present
	}

	// Every remaining operator compares a value, so an absent field cannot
	// match — including not_equals. Matching "absent" via not_equals would make
	// a single typo'd attribute key drop the entire pipeline; use not_exists
	// when absence is what you mean.
	if !present {
		return false
	}
	if c.ignoreCase {
		got = strings.ToLower(got)
	}

	switch c.op {
	case opEquals:
		return c.anyOf(got)
	case opNotEquals:
		return !c.anyOf(got)
	case opContains:
		return strings.Contains(got, c.value)
	case opPrefix:
		return strings.HasPrefix(got, c.value)
	case opSuffix:
		return strings.HasSuffix(got, c.value)
	case opRegex:
		return c.re.MatchString(got)
	}
	return false
}

// anyOf reports whether got equals Value or any entry of Values.
func (c *compiledCond) anyOf(got string) bool {
	if len(c.values) > 0 {
		for _, v := range c.values {
			if got == v {
				return true
			}
		}
		return false
	}
	return got == c.value
}

// matches reports whether every condition holds (AND) and the rule has not
// expired. `now` is passed in rather than read from the clock per rule so a
// batch is evaluated against one consistent instant.
func (r *compiledRule) matches(e *evalCtx, now time.Time) bool {
	if !r.expiresAt.IsZero() && now.After(r.expiresAt) {
		return false
	}
	for i := range r.conds {
		if !r.conds[i].eval(e) {
			return false
		}
	}
	return true
}

// match returns the first rule that claims this item, or nil. Keep rules sort
// first, so an explicit exception always wins over any drop rule.
func (rs *ruleSet) match(signal string, e *evalCtx, now time.Time) *compiledRule {
	for _, r := range rs.rulesFor(signal) {
		if r.matches(e, now) {
			return r
		}
	}
	return nil
}

// shouldDrop is the data-path entry point: true when the item is claimed by a
// drop rule. Returns the rule id for the per-rule drop counter.
func (rs *ruleSet) shouldDrop(signal string, e *evalCtx, now time.Time) (string, bool) {
	r := rs.match(signal, e, now)
	if r == nil || r.action != actionDrop {
		return "", false
	}
	return r.id, true
}

// compile turns a raw rule document into its hot-path form, rejecting anything
// ambiguous. Errors are per-rule: one malformed document must not blank the
// whole rule set, which would silently disable every other drop rule.
func compile(raw []byte, id string) (*compiledRule, []string, error) {
	var r Rule
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, nil, fmt.Errorf("invalid JSON: %w", err)
	}
	if r.ID == "" {
		r.ID = id
	}
	if r.Enabled != nil && !*r.Enabled {
		return nil, nil, nil // disabled: not an error, just not loaded
	}

	action := r.Action
	if action == "" {
		action = actionDrop
	}
	if action != actionDrop && action != actionKeep {
		return nil, nil, fmt.Errorf("action must be %q or %q, got %q", actionDrop, actionKeep, action)
	}

	signals, err := normalizeSignals(r.Signals)
	if err != nil {
		return nil, nil, err
	}

	if len(r.Conditions) == 0 {
		return nil, nil, fmt.Errorf("conditions must not be empty (a rule matching everything is refused)")
	}

	cr := &compiledRule{id: r.ID, action: action, conds: make([]compiledCond, 0, len(r.Conditions))}

	if r.ExpiresAt != "" {
		exp, err := time.Parse(time.RFC3339, r.ExpiresAt)
		if err != nil {
			return nil, nil, fmt.Errorf("expires_at must be RFC3339: %w", err)
		}
		cr.expiresAt = exp
	}

	for i, c := range r.Conditions {
		cc, err := compileCondition(c)
		if err != nil {
			return nil, nil, fmt.Errorf("conditions[%d]: %w", i, err)
		}
		cr.conds = append(cr.conds, cc)
	}

	return cr, signals, nil
}

func compileCondition(c Condition) (compiledCond, error) {
	cc := compiledCond{
		source:     c.Source,
		key:        c.Key,
		op:         c.Op,
		value:      c.Value,
		values:     c.Values,
		ignoreCase: c.IgnoreCase,
	}
	if cc.op == "" {
		cc.op = opEquals
	}

	switch cc.source {
	case sourceResource, sourceAttribute:
		if cc.key == "" {
			return cc, fmt.Errorf("key is required for source %q", cc.source)
		}
	case sourceName, sourceBody, sourceSeverity, sourceScope:
	case "":
		return cc, fmt.Errorf("source is required (one of resource, attribute, name, body, severity, scope)")
	default:
		return cc, fmt.Errorf("unknown source %q", cc.source)
	}

	switch cc.op {
	case opExists, opNotExists:
		return cc, nil
	case opEquals, opNotEquals:
		if cc.value == "" && len(cc.values) == 0 {
			return cc, fmt.Errorf("op %q requires value or values", cc.op)
		}
	case opContains, opPrefix, opSuffix:
		if cc.value == "" {
			return cc, fmt.Errorf("op %q requires value", cc.op)
		}
	case opRegex:
		if cc.value == "" {
			return cc, fmt.Errorf("op %q requires value", cc.op)
		}
		re, err := regexp.Compile(cc.value)
		if err != nil {
			return cc, fmt.Errorf("invalid regex %q: %w", cc.value, err)
		}
		cc.re = re
		return cc, nil
	default:
		return cc, fmt.Errorf("unknown op %q", cc.op)
	}

	// Fold the needle once at compile time; eval only folds the haystack.
	if cc.ignoreCase {
		cc.value = strings.ToLower(cc.value)
		for i := range cc.values {
			cc.values[i] = strings.ToLower(cc.values[i])
		}
	}
	return cc, nil
}

func normalizeSignals(in []string) ([]string, error) {
	if len(in) == 0 {
		return []string{signalTraces, signalMetrics, signalLogs}, nil
	}
	out := make([]string, 0, len(in))
	for _, s := range in {
		switch strings.ToLower(s) {
		case signalTraces, signalMetrics, signalLogs:
			out = append(out, strings.ToLower(s))
		default:
			return nil, fmt.Errorf("unknown signal %q (want traces, metrics or logs)", s)
		}
	}
	return out, nil
}

// buildRuleSet compiles a whole Redis hash into a snapshot. Rules are processed
// in sorted-id order so that the max_rules cap truncates deterministically —
// every replica must end up with the same rule set, otherwise the same span is
// dropped by one collector and kept by another.
func buildRuleSet(docs map[string]string, version int64, maxRules int, onInvalid func(id string, err error)) *ruleSet {
	rs := &ruleSet{version: version, loadedA: time.Now()}

	ids := make([]string, 0, len(docs))
	for id := range docs {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	loaded := 0
	for _, id := range ids {
		if maxRules >= 0 && loaded >= maxRules {
			rs.invalid += len(ids) - loaded
			if onInvalid != nil {
				onInvalid(id, fmt.Errorf("max_rules (%d) reached; remaining rules ignored", maxRules))
			}
			break
		}
		cr, signals, err := compile([]byte(docs[id]), id)
		if err != nil {
			rs.invalid++
			if onInvalid != nil {
				onInvalid(id, err)
			}
			continue
		}
		if cr == nil {
			continue // explicitly disabled
		}
		loaded++
		for _, s := range signals {
			switch s {
			case signalTraces:
				rs.traces = append(rs.traces, cr)
			case signalMetrics:
				rs.metrics = append(rs.metrics, cr)
			case signalLogs:
				rs.logs = append(rs.logs, cr)
			}
		}
	}
	rs.total = loaded

	// Keep rules are exceptions and must be considered before any drop rule.
	// sort.SliceStable preserves the id ordering within each group, so the
	// evaluation order stays reproducible across replicas.
	for _, list := range [][]*compiledRule{rs.traces, rs.metrics, rs.logs} {
		sortKeepFirst(list)
	}

	for _, r := range rs.logs {
		for i := range r.conds {
			if r.conds[i].source == sourceBody {
				rs.needsBody = true
			}
		}
	}

	return rs
}

func sortKeepFirst(rules []*compiledRule) {
	sort.SliceStable(rules, func(i, j int) bool {
		return rules[i].action == actionKeep && rules[j].action != actionKeep
	})
}
