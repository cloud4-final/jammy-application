"""Configure the classic S3 transfer client's bandwidth limiter."""

from boto3.s3.transfer import TransferConfig


def upload_config(bytes_per_second: int) -> TransferConfig:
    return TransferConfig(
        max_bandwidth=bytes_per_second or None,
        preferred_transfer_client="classic",
    )
