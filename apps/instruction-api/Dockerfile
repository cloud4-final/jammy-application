FROM ghcr.io/astral-sh/uv:0.12.24@sha256:3af4716e991d6956a41e573eab705d0ee08500cd829ed30293eb8472f372c65a AS uv
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
COPY --from=uv /uv /bin/uv
ENV UV_PYTHON_DOWNLOADS=never UV_LINK_MODE=copy
WORKDIR /workspace
COPY --from=manifests /manifests/ ./
COPY libs/py/jammy_common/ libs/py/jammy_common/
COPY apps/instruction-api/src/ apps/instruction-api/src/
RUN if [ -f apps/instruction-api/pyproject.toml ] && [ -f apps/dispatcher/pyproject.toml ] && [ -f apps/portal-publisher/pyproject.toml ] && [ -f apps/ingest-py/pyproject.toml ] && [ -f apps/processor-py/pyproject.toml ] && [ -f apps/archiver-py/pyproject.toml ] && [ -f tests/e2e/pyproject.toml ]; then \
      uv sync --locked --no-dev --no-editable --package instruction-api; \
    else \
      uv sync --frozen --no-dev --no-editable --package instruction-api; \
    fi

FROM python:3.14.10-slim-trixie@sha256:970b9cf6bb33bc54e34bff9263de57460841813dd5a3d83d3a4e183b21c54f8a
ARG IMAGE_VERSION=dev
ARG IMAGE_REVISION=unknown
LABEL org.opencontainers.image.title="instruction-api" \
      org.opencontainers.image.description="Serves the Jammy instruction API." \
      org.opencontainers.image.source="https://github.com/cloud4-final/jammy-application" \
      org.opencontainers.image.version="${IMAGE_VERSION}" \
      org.opencontainers.image.revision="${IMAGE_REVISION}"
# Apply Debian security fixes for the affected runtime packages.
RUN apt-get update \
    && apt-get install -y --no-install-recommends --only-upgrade zlib1g libzstd1 \
    && rm -rf /var/lib/apt/lists/* \
        /usr/local/bin/pip /usr/local/bin/pip3 /usr/local/bin/pip3.14 \
        /usr/local/lib/python3.14/ensurepip \
        /usr/local/lib/python3.14/site-packages/pip \
        /usr/local/lib/python3.14/site-packages/pip-*.dist-info \
        /usr/local/lib/python3.14/site-packages/setuptools \
        /usr/local/lib/python3.14/site-packages/setuptools-*.dist-info
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY --from=build /workspace/.venv /workspace/.venv
USER 65532:65532
EXPOSE 8080 9090
ENTRYPOINT ["/workspace/.venv/bin/python", "-m", "instruction_api.main"]
