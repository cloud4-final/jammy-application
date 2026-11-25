"""Configure the classic S3 transfer client's bandwidth limiter."""

import math

from boto3.s3.transfer import TransferConfig

MIN_PART_SIZE = 64 * 1024**2
MAX_PARTS = 10_000


def multipart_part_size(size: int) -> int:
    if size <= 0:
        return MIN_PART_SIZE
    return max(MIN_PART_SIZE, math.ceil(size / MAX_PARTS))


def upload_config(bytes_per_second: int, size: int = 0) -> TransferConfig:
    return TransferConfig(
        multipart_threshold=MIN_PART_SIZE,
        multipart_chunksize=multipart_part_size(size),
        max_bandwidth=bytes_per_second or None,
        preferred_transfer_client="classic",
    )
