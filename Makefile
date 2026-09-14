OCB_VERSION := v0.160.0
MDATAGEN_VERSION := v0.160.0
BINARY := cmd/otelcol-gateway/otelcol-gateway
CONFIG := config/otelcol-gateway.yaml

.PHONY: install-builder install-mdatagen install-telemetrygen generate build run clean test test-unit test-e2e validate docker-build

install-builder:
	go install go.opentelemetry.io/collector/cmd/builder@$(OCB_VERSION)

install-mdatagen:
	go install go.opentelemetry.io/collector/cmd/mdatagen@$(MDATAGEN_VERSION)

install-telemetrygen:
	go install github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen@$(OCB_VERSION)

# Regenerates internal/metadata, documentation.md and the generated component
# tests from each processor's metadata.yaml. Run after editing metadata.yaml.
generate:
	cd processor/ratelimitprocessor && mdatagen metadata.yaml
	cd processor/statefulfilterprocessor && mdatagen metadata.yaml

build:
	builder --config builder-config.yaml

run: build
	$(BINARY) --config $(CONFIG)

clean:
	rm -f $(BINARY)

test-unit:
	cd processor/ratelimitprocessor && go test -race -count=1 ./...
	cd processor/statefulfilterprocessor && go test -race -count=1 ./...

# Pushes real telemetry through the built binary and asserts the counters add
# up. Unit tests cover the processors as libraries; this covers the artifact
# OCB regenerates on every bump. Needs telemetrygen, and Docker for the Redis
# scenarios (skipped without it).
test-e2e: build
	scripts/e2e.sh

test: test-unit

# Validates the default config against the built binary (catches schema drift).
validate: build
	$(BINARY) validate --config $(CONFIG)

docker-build:
	docker build -t otelcol-gateway:dev .
