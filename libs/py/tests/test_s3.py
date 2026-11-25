import pytest

from jammy_common import s3
from jammy_common.config import CommonConfig, ConfigError


@pytest.mark.parametrize("endpoint", ["not-a-url", "http://:9000", "ftp://storage:9000"])
def test_s3_client_rejects_invalid_endpoint_before_loading_credentials(monkeypatch, endpoint):
    def unexpected_client(*args, **kwargs):
        pytest.fail("boto3 must not be initialized with an invalid endpoint")

    monkeypatch.setenv("SITE_ID", "site-a")
    monkeypatch.setenv("S3_ENDPOINT", endpoint)
    monkeypatch.setattr(s3.boto3, "client", unexpected_client)

    with pytest.raises(ConfigError, match="^invalid S3_ENDPOINT$"):
        s3.client(CommonConfig.from_env())


def test_s3_client_sets_explicit_timeouts_and_retries(monkeypatch):
    monkeypatch.setenv("SITE_ID", "site-a")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("S3_FORCE_PATH_STYLE", "true")

    client = s3.client(CommonConfig.from_env())
    cfg = client._client_config
    assert cfg.connect_timeout == 2
    assert cfg.read_timeout == 15
    assert cfg.retries == {"total_max_attempts": 3, "mode": "standard"}
    assert cfg.s3 == {"addressing_style": "path"}

    monkeypatch.setenv("S3_FORCE_PATH_STYLE", "false")
    default_client = s3.client(CommonConfig.from_env())
    assert default_client._client_config.connect_timeout == 2
    assert default_client._client_config.read_timeout == 15
    assert default_client._client_config.retries == {
        "total_max_attempts": 3,
        "mode": "standard",
    }
    assert not default_client._client_config.s3

    monkeypatch.setenv("S3_FORCE_PATH_STYLE", "maybe")
    with pytest.raises(ConfigError, match="S3_FORCE_PATH_STYLE"):
        s3.client(CommonConfig.from_env())


def test_s3_prefix_normalization_and_file_override(monkeypatch, tmp_path):
    for raw, want in (
        ("", ""),
        ("/", ""),
        ("///", ""),
        ("site-a", "site-a/"),
        ("/site-a/prod/", "site-a/prod/"),
    ):
        monkeypatch.setenv("S3_PREFIX", raw)
        assert s3.prefix() == want

    secret = tmp_path / "prefix"
    secret.write_text("/from-file/\n", encoding="utf-8")
    monkeypatch.setenv("S3_PREFIX_FILE", str(secret))
    assert s3.prefix() == "from-file/"
