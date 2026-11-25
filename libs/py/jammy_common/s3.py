"""Create the S3 client using AWS's standard credential provider chain."""

import boto3
from botocore.config import Config

from .config import CommonConfig, ConfigError, env


def client(cfg: CommonConfig):
    path_style = env("S3_FORCE_PATH_STYLE", "false")
    if path_style not in {"true", "false"}:
        raise ConfigError("S3_FORCE_PATH_STYLE must be true or false")
    config = Config(s3={"addressing_style": "path"}) if path_style == "true" else None
    return boto3.client(
        "s3",
        endpoint_url=env("S3_ENDPOINT") or None,
        region_name=env("S3_REGION", "ap-northeast-2"),
        config=config,
    )
