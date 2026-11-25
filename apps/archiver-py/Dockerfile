FROM python:3.14.10-slim-trixie@sha256:970b9cf6bb33bc54e34bff9263de57460841813dd5a3d83d3a4e183b21c54f8a AS manifests
WORKDIR /manifests
RUN --mount=type=bind,target=/ctx \
    cp /ctx/pyproject.toml /ctx/uv.lock ./ && \
    for d in /ctx/libs/* /ctx/apps/* /ctx/tests/*; do \
      if [ -f "$d/pyproject.toml" ]; then \
        rel=${d#/ctx/}; \
        mkdir -p "$rel" && cp "$d/pyproject.toml" "$rel/"; \
      fi; \
    done

FROM python:3.14.10-slim-trixie@sha256:970b9cf6bb33bc54e34bff9263de57460841813dd5a3d83d3a4e183b21c54f8a AS build
COPY --from=ghcr.io/astral-sh/uv:0.12.24@sha256:3af4716e991d6956a41e573eab705d0ee08500cd829ed30293eb8472f372c65a /uv /bin/uv
WORKDIR /app
COPY --from=manifests /manifests/ ./
COPY libs/py ./libs/py
COPY apps/archiver-py/src ./apps/archiver-py/src
RUN if [ -f apps/instruction-api/pyproject.toml ] && [ -f apps/dispatcher/pyproject.toml ] && [ -f apps/portal-publisher/pyproject.toml ] && [ -f apps/ingest-py/pyproject.toml ] && [ -f apps/processor-py/pyproject.toml ] && [ -f apps/archiver-py/pyproject.toml ] && [ -f tests/e2e/pyproject.toml ]; then \
      uv sync --locked --no-dev --no-editable --package archiver-py; \
    else \
      uv sync --frozen --no-dev --no-editable --package archiver-py; \
    fi

FROM python:3.14.10-slim-trixie@sha256:970b9cf6bb33bc54e34bff9263de57460841813dd5a3d83d3a4e183b21c54f8a
ARG IMAGE_VERSION=dev
ARG IMAGE_REVISION=unknown
LABEL org.opencontainers.image.title="archiver-py" \
      org.opencontainers.image.description="Archives Jammy event batches as Parquet objects using Python." \
      org.opencontainers.image.source="https://github.com/cloud4-final/jammy-application" \
      org.opencontainers.image.version="${IMAGE_VERSION}" \
      org.opencontainers.image.revision="${IMAGE_REVISION}"
# Apply Debian security fixes for the affected runtime packages.
RUN apt-get update \
    && apt-get install -y --no-install-recommends --only-upgrade zlib1g \
    && rm -rf /var/lib/apt/lists/* \
        /usr/local/bin/pip /usr/local/bin/pip3 /usr/local/bin/pip3.14 \
        /usr/local/lib/python3.14/ensurepip \
        /usr/local/lib/python3.14/site-packages/pip \
        /usr/local/lib/python3.14/site-packages/pip-*.dist-info \
        /usr/local/lib/python3.14/site-packages/setuptools \
        /usr/local/lib/python3.14/site-packages/setuptools-*.dist-info
ENV PATH="/app/.venv/bin:${PATH}" PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY --from=build /app/.venv /app/.venv
RUN install -d -o 65532 -g 65532 /stage
USER 65532:65532
EXPOSE 9090
ENTRYPOINT ["jammy-archiver"]
