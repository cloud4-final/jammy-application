FROM --platform=$BUILDPLATFORM golang:1.27.4-trixie@sha256:c393c00864b56f4404b3fe4d029e47742fbb53ee11506521d2f3377009ad7a3a AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY libs/go/ ./libs/go/
COPY apps/archiver-go/ ./apps/archiver-go/
ARG TARGETOS TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -o /out/archiver ./apps/archiver-go \
    && mkdir /out/stage

FROM gcr.io/distroless/static-debian13:nonroot@sha256:8ce6dd87e9823c5b48dcfb2c8604714ccb351ce59607327bcc4957448da279b5
ARG IMAGE_VERSION=dev
ARG IMAGE_REVISION=unknown
LABEL org.opencontainers.image.title="archiver-go" \
      org.opencontainers.image.description="Archives Jammy event batches as Parquet objects using Go." \
      org.opencontainers.image.source="https://github.com/cloud4-final/jammy-application" \
      org.opencontainers.image.version="${IMAGE_VERSION}" \
      org.opencontainers.image.revision="${IMAGE_REVISION}"
COPY --from=build /out/archiver /archiver
COPY --from=build --chown=65532:65532 /out/stage /stage
EXPOSE 9090
USER 65532:65532
ENTRYPOINT ["/archiver"]
