package statefulfilterprocessor

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/component"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
)

// countingStore records how often the shared lifecycle hooks actually fire on
// the underlying store, which is the whole point of the registry.
type countingStore struct {
	staticStore
	loads  int
	starts int
	closes int
}

func (s *countingStore) initialLoad(context.Context) error { s.loads++; return nil }
func (s *countingStore) start()                            { s.starts++ }
func (s *countingStore) close() error                      { s.closes++; return nil }

func TestRegistry_SharesOneStoreAcrossSignals(t *testing.T) {
	reg := newStoreRegistry()
	id := component.MustNewID("statefulfilter")
	underlying := &countingStore{staticStore: staticStore{rs: emptyRuleSet()}}

	builds := 0
	build := func() (ruleStore, error) {
		builds++
		return underlying, nil
	}

	// Three signal pipelines of the same component instance.
	handles := make([]ruleStore, 3)
	for i := range handles {
		h, err := reg.acquire(id, build)
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}
		handles[i] = h
	}

	if builds != 1 {
		t.Fatalf("expected one Redis connection/poller for the component, got %d", builds)
	}

	for _, h := range handles {
		if err := h.initialLoad(context.Background()); err != nil {
			t.Fatalf("initialLoad: %v", err)
		}
		h.start()
	}
	if underlying.loads != 1 {
		t.Fatalf("expected a single initial load, got %d", underlying.loads)
	}
	if underlying.starts != 1 {
		t.Fatalf("expected a single refresh loop, got %d", underlying.starts)
	}

	// The store survives until the last pipeline shuts down.
	for i, h := range handles {
		if err := h.close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		want := 0
		if i == len(handles)-1 {
			want = 1
		}
		if underlying.closes != want {
			t.Fatalf("after closing handle %d: closes=%d, want %d", i, underlying.closes, want)
		}
	}
}

func TestRegistry_GaugesRegisteredOncePerStore(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	reg := newStoreRegistry()
	id := component.MustNewID("statefulfilter")
	shared := storeWith(map[string]string{
		"a": `{"conditions":[{"source":"name","value":"x"}]}`,
	})
	build := func() (ruleStore, error) { return shared, nil }

	// Traces, metrics and logs processors of the same component instance.
	for range 3 {
		h, err := reg.acquire(id, build)
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}
		if _, err := newStatefulFilterProcessor(testConfig(), zap.NewNop(), mp, h); err != nil {
			t.Fatalf("newStatefulFilterProcessor: %v", err)
		}
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}

	// gaugeValue fails unless there is exactly one data point: three identical
	// callbacks would emit three for the same attribute-free gauge.
	if got := gaugeValue(t, &rm, "otelcol_processor_statefulfilter_rules_loaded"); got != 1 {
		t.Fatalf("expected rules_loaded=1, got %d", got)
	}
}

func TestRegistry_SeparateComponentIDsAreIndependent(t *testing.T) {
	reg := newStoreRegistry()
	builds := 0
	build := func() (ruleStore, error) {
		builds++
		return &countingStore{staticStore: staticStore{rs: emptyRuleSet()}}, nil
	}

	// Two `statefulfilter/<name>` instances may point at different rule prefixes,
	// so they must not share a store.
	if _, err := reg.acquire(component.MustNewIDWithName("statefulfilter", "a"), build); err != nil {
		t.Fatalf("acquire a: %v", err)
	}
	if _, err := reg.acquire(component.MustNewIDWithName("statefulfilter", "b"), build); err != nil {
		t.Fatalf("acquire b: %v", err)
	}
	if builds != 2 {
		t.Fatalf("expected one store per component ID, got %d", builds)
	}
}

// Compile-time guard: if sharedStore ever stops satisfying gaugeRegistrar, the
// processor silently falls back to per-pipeline registration and the gauges
// double up again.
var _ gaugeRegistrar = (*sharedStore)(nil)
