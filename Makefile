OCB_VERSION := v0.159.0
BINARY := cmd/otelcol-gateway/otelcol-gateway
CONFIG := config/otelcol-gateway.yaml

.PHONY: install-builder build run clean test test-unit validate docker-build

install-builder:
	go install go.opentelemetry.io/collector/cmd/builder@$(OCB_VERSION)

build:
	builder --config builder-config.yaml

run: build
	$(BINARY) --config $(CONFIG)

clean:
	rm -f $(BINARY)

test-unit:
	cd processor/ratelimitprocessor && go test -race -count=1 ./...
	cd processor/statefulfilterprocessor && go test -race -count=1 ./...

test: test-unit

# Validates the default config against the built binary (catches schema drift).
validate: build
	$(BINARY) validate --config $(CONFIG)

docker-build:
	docker build -t otelcol-gateway:dev .
