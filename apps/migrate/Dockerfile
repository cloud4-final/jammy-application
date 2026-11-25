FROM --platform=$BUILDPLATFORM golang:1.27.4-trixie@sha256:c393c00864b56f4404b3fe4d029e47742fbb53ee11506521d2f3377009ad7a3a AS build
ARG TARGETOS TARGETARCH
ENV CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH GOTOOLCHAIN=local
WORKDIR /src
COPY go.mod go.sum ./
COPY libs/go/config/ libs/go/config/
COPY apps/migrate/main.go apps/migrate/main.go
# Upstream registers its test stub even when no database tags select it.
RUN version=v4.20.1 \
    && module=github.com/golang-migrate/migrate/v4 \
    && go mod download "${module}@${version}" \
    && cp -R --no-preserve=mode "$(go env GOMODCACHE)/${module}@${version}" /src/upstream \
    && (cd /src/upstream \
        && sed -i '/database\/stub/d' internal/cli/commands.go \
        && go build -trimpath -tags postgres \
            -ldflags="-X main.Version=${version}" -o /out/migrate ./cmd/migrate) \
    && go build -trimpath -o /out/migrate-from-env ./apps/migrate

FROM gcr.io/distroless/static-debian13:nonroot@sha256:8ce6dd87e9823c5b48dcfb2c8604714ccb351ce59607327bcc4957448da279b5
ARG IMAGE_VERSION=dev
ARG IMAGE_REVISION=unknown
LABEL org.opencontainers.image.title="migrate" \
      org.opencontainers.image.description="Applies the Jammy PostgreSQL schema migrations." \
      org.opencontainers.image.source="https://github.com/cloud4-final/jammy-application" \
      org.opencontainers.image.version="${IMAGE_VERSION}" \
      org.opencontainers.image.revision="${IMAGE_REVISION}"
COPY --from=build /out/migrate /migrate
COPY --from=build /out/migrate-from-env /migrate-from-env
COPY contracts/db/migrations/ /migrations/
USER 65532:65532
ENTRYPOINT ["/migrate"]
