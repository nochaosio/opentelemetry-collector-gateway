# syntax=docker/dockerfile:1.7
#
# Multi-stage build: compile via OCB, ship a distroless image.
# Final image is non-root and contains only the collector binary
# and CA certs — nothing else.

ARG GO_VERSION=1.26
# Keep in sync with OCB_VERSION in the Makefile and the collector
# versions pinned in builder-config.yaml.
ARG OCB_VERSION=v0.159.0

FROM golang:${GO_VERSION}-alpine AS build
ARG OCB_VERSION
RUN apk add --no-cache git ca-certificates binutils \
 && go install go.opentelemetry.io/collector/cmd/builder@${OCB_VERSION}

WORKDIR /src
# Copy the custom processors' module files first for better layer caching.
COPY processor/ratelimitprocessor/go.mod processor/ratelimitprocessor/go.sum \
     processor/ratelimitprocessor/
COPY processor/statefulfilterprocessor/go.mod processor/statefulfilterprocessor/go.sum \
     processor/statefulfilterprocessor/
COPY . .
RUN builder --config builder-config.yaml \
 && strip cmd/otelcol-gateway/otelcol-gateway

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /src/cmd/otelcol-gateway/otelcol-gateway /otelcol-gateway
COPY --from=build /src/config/otelcol-gateway.yaml /etc/otelcol-gateway/config.yaml

# OTLP gRPC / HTTP, health_check, prometheus self-telemetry.
# pprof (1777) binds to localhost by default and is intentionally NOT exposed.
EXPOSE 4317 4318 13133 8888

USER nonroot:nonroot
ENTRYPOINT ["/otelcol-gateway"]
CMD ["--config", "/etc/otelcol-gateway/config.yaml"]
