import pytest

from jammy_common import logging, obs
from jammy_common.config import CommonConfig, ConfigError


@pytest.mark.parametrize(
    "endpoint",
    [
        "not-a-url",
        "   ",
        "ftp://collector:4317",
        "http://",
        "http://:4317",
        "http://bad host:4317",
        "http://collector:notaport",
        "http://[::1",
    ],
)
def test_invalid_otlp_endpoint_rejected(monkeypatch, endpoint):
    monkeypatch.setenv("SITE_ID", "site-a")
    monkeypatch.setenv("INSTANCE_ID", "instance-a")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint)
    cfg = CommonConfig.from_env()

    with pytest.raises(ConfigError, match="^invalid OTEL_EXPORTER_OTLP_ENDPOINT$"):
        obs.setup("ingest-py", cfg)
    with pytest.raises(ConfigError, match="^invalid OTEL_EXPORTER_OTLP_ENDPOINT$"):
        obs.setup_tracing("ingest-py", cfg)
    with pytest.raises(ConfigError, match="^invalid OTEL_EXPORTER_OTLP_ENDPOINT$"):
        logging.setup("ingest-py", cfg)


@pytest.mark.parametrize(
    "endpoint",
    [
        "",
        "http://collector:4317",
        "https://otel.example.com",
        "HTTP://[::1]:4317",
    ],
)
def test_valid_or_empty_otlp_endpoint_accepted(monkeypatch, endpoint):
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint)
    assert obs.otlp_endpoint() == endpoint


def test_invalid_resource_attributes_fail_without_echoing_the_value(monkeypatch, caplog):
    monkeypatch.setenv("SITE_ID", "site-a")
    monkeypatch.setenv("OTEL_RESOURCE_ATTRIBUTES", "service.version=1.0.0,malformed-value")

    with pytest.raises(ConfigError, match="^invalid OTEL_RESOURCE_ATTRIBUTES$"):
        obs.setup("ingest-py", CommonConfig.from_env())
    assert "malformed-value" not in caplog.text
