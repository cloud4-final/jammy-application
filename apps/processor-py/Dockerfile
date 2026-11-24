FROM python:3.14.10-slim-trixie@sha256:970b9cf6bb33bc54e34bff9263de57460841813dd5a3d83d3a4e183b21c54f8a AS build
RUN python -m pip install --no-cache-dir uv==0.12.24
WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY libs/py ./libs/py
COPY apps/instruction-api/pyproject.toml ./apps/instruction-api/
COPY apps/dispatcher/pyproject.toml ./apps/dispatcher/
COPY apps/portal-publisher/pyproject.toml ./apps/portal-publisher/
COPY apps/ingest-py/pyproject.toml ./apps/ingest-py/
COPY apps/archiver-py/pyproject.toml ./apps/archiver-py/
COPY tests/e2e/pyproject.toml ./tests/e2e/
COPY apps/processor-py/pyproject.toml ./apps/processor-py/
COPY apps/processor-py/src ./apps/processor-py/src
RUN uv sync --locked --no-dev --no-editable --package processor-py

FROM python:3.14.10-slim-trixie@sha256:970b9cf6bb33bc54e34bff9263de57460841813dd5a3d83d3a4e183b21c54f8a
ARG IMAGE_VERSION=dev
ARG IMAGE_REVISION=unknown
LABEL org.opencontainers.image.title="processor-py" \
      org.opencontainers.image.description="Processes Jammy event batches into operational summaries using Python." \
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
USER 65532:65532
EXPOSE 9090
ENTRYPOINT ["jammy-processor"]
