from time import monotonic

from archiver.ratelimit import upload_config
from test_archiver_jetstream import (  # noqa: F401 - pytest fixtures
    _s3_fixture,
    _seaweed_endpoint,
)


def test_upload_rate_limit_respected(tmp_path, s3_fixture):
    file = tmp_path / "five-mebibytes"
    file.write_bytes(b"a" * 5 * 1024**2)
    client, bucket, _ = s3_fixture
    start = monotonic()
    client.upload_file(str(file), bucket, "bandwidth", Config=upload_config(1024**2))
    assert monotonic() - start >= 4.5
    assert (
        client.head_object(Bucket=bucket, Key="bandwidth")["ContentLength"]
        == file.stat().st_size
    )
