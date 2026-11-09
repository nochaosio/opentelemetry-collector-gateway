# syntax=docker/dockerfile:1.7
#
# Multi-stage build: compile via OCB, ship a distroless image.
# Final image is non-root and contains only the collector binary
# and CA certs — nothing else.

ARG GO_VERSION=1.27
# Keep in sync with OCB_VERSION in the Makefile and the collector
# versions pinned in builder-config.yaml.
ARG OCB_VERSION=v0.165.0

# The build stage runs on the host's architecture and cross-compiles for the
# target one: running OCB under QEMU for arm64 is several times slower.
FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine AS build
ARG OCB_VERSION
RUN apk add --no-cache git ca-certificates \
 && go install go.opentelemetry.io/collector/cmd/builder@${OCB_VERSION}

WORKDIR /src
# Copy the custom processors' module files first for better layer caching.
COPY processor/ratelimitprocessor/go.mod processor/ratelimitprocessor/go.sum \
     processor/ratelimitprocessor/
COPY processor/statefulfilterprocessor/go.mod processor/statefulfilterprocessor/go.sum \
     processor/statefulfilterprocessor/
COPY . .
ARG TARGETOS TARGETARCH
# OCB already links with -s -w unless debug_compilation is set, so no strip.
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    builder --config builder-config.yaml

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /src/cmd/otelcol-gateway/otelcol-gateway /otelcol-gateway
COPY --from=build /src/config/otelcol-gateway.yaml /etc/otelcol-gateway/config.yaml

# OTLP gRPC / HTTP, health_check, prometheus self-telemetry.
# The front-door receivers (jaeger, zipkin, ...) use the ports in
# config/otelcol-gateway-frontdoor.yaml; publish the ones you enable.
# pprof (1777) binds to localhost by default and is intentionally NOT exposed.
EXPOSE 4317 4318 13133 8888

USER nonroot:nonroot
ENTRYPOINT ["/otelcol-gateway"]
CMD ["--config", "/etc/otelcol-gateway/config.yaml"]
