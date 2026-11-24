FROM --platform=$BUILDPLATFORM golang:1.27.4-trixie AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
COPY libs/go/pb ./libs/go/pb
COPY tools/payloadgen/gen ./tools/payloadgen/gen
COPY loadtest/perf/loadgen ./loadtest/perf/loadgen
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -o /loadgen ./loadtest/perf/loadgen

FROM gcr.io/distroless/static-debian13:nonroot@sha256:8ce6dd87e9823c5b48dcfb2c8604714ccb351ce59607327bcc4957448da279b5
COPY --from=build /loadgen /loadgen
USER 65532:65532
ENTRYPOINT ["/loadgen"]
