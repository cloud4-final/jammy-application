"""Create the S3 client using AWS's standard credential provider chain."""

import boto3
from botocore.config import Config

from .config import CommonConfig, ConfigError, env, validate_http_endpoint


def normalize_prefix(raw: str) -> str:
    trimmed = raw.strip("/")
    return f"{trimmed}/" if trimmed else ""


def prefix() -> str:
    return normalize_prefix(env("S3_PREFIX", ""))


def client(cfg: CommonConfig):
    endpoint = env("S3_ENDPOINT")
    if endpoint:
        validate_http_endpoint("S3_ENDPOINT", endpoint)
    path_style = env("S3_FORCE_PATH_STYLE", "false")
    if path_style not in {"true", "false"}:
        raise ConfigError("S3_FORCE_PATH_STYLE must be true or false")
    options: dict[str, object] = {
        "connect_timeout": 2,
        "read_timeout": 15,
        "retries": {"max_attempts": 2, "mode": "standard"},
    }
    if path_style == "true":
        options["s3"] = {"addressing_style": "path"}
    return boto3.client(
        "s3",
        endpoint_url=endpoint or None,
        region_name=env("S3_REGION", "ap-northeast-2"),
        config=Config(**options),
    )
