"""Create or check the AWS-compatible resources on the local Compose network."""
import sys
import time

import boto3
from botocore.config import Config
from botocore.exceptions import (
    ClientError,
    ConnectTimeoutError,
    EndpointConnectionError,
    ReadTimeoutError,
)

REGION = "ap-northeast-2"
BUCKET = "jammy-local"
QUEUE = "jammy-instructions"
TABLE = "jammy-site-lease"
TIMEOUT_ERRORS = (ConnectTimeoutError, ReadTimeoutError)


def client(service, endpoint):
    return boto3.client(
        service,
        endpoint_url=endpoint,
        region_name=REGION,
        aws_access_key_id="jammy",
        aws_secret_access_key="jammy",
        config=Config(
            connect_timeout=2,
            read_timeout=3,
            retries={"total_max_attempts": 1},
            s3={"addressing_style": "path"},
        ),
    )


def initialize(operation):
    deadline = time.monotonic() + 60
    while True:
        try:
            operation()
            return
        except (EndpointConnectionError, *TIMEOUT_ERRORS):
            if time.monotonic() >= deadline:
                raise
        except ClientError as error:
            if error.response["ResponseMetadata"]["HTTPStatusCode"] < 500 or time.monotonic() >= deadline:
                raise
        time.sleep(1)


def create_bucket(s3):
    try:
        s3.create_bucket(
            Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": REGION}
        )
    except s3.exceptions.BucketAlreadyOwnedByYou:
        pass


def create_table(dynamodb):
    try:
        dynamodb.create_table(
            TableName=TABLE,
            AttributeDefinitions=[{"AttributeName": "lease_name", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "lease_name", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
        )
    except dynamodb.exceptions.ResourceInUseException:
        pass
    except TIMEOUT_ERRORS as error:
        # CreateTable may have succeeded before its response timed out.
        try:
            dynamodb.describe_table(TableName=TABLE)
        except dynamodb.exceptions.ResourceNotFoundException:
            raise error from None


def main(action):
    s3 = client("s3", "http://seaweedfs:8333")
    sqs = client("sqs", "http://elasticmq:9324")
    dynamodb = client("dynamodb", "http://dynamodb-local:8000")

    if action == "init":
        initialize(lambda: create_bucket(s3))
        initialize(lambda: sqs.create_queue(QueueName=QUEUE))
        initialize(lambda: create_table(dynamodb))
        waiter = dynamodb.get_waiter("table_exists")
        initialize(lambda: waiter.wait(
            TableName=TABLE, WaiterConfig={"Delay": 1, "MaxAttempts": 60}
        ))

    s3.list_objects_v2(Bucket=BUCKET, MaxKeys=1)
    print(f"PASS: S3 bucket {BUCKET} is listable")
    sqs.get_queue_url(QueueName=QUEUE)
    print(f"PASS: SQS queue {QUEUE} exists")
    table = dynamodb.describe_table(TableName=TABLE)["Table"]
    if table["TableStatus"] != "ACTIVE" or table["KeySchema"] != [
        {"AttributeName": "lease_name", "KeyType": "HASH"}
    ]:
        raise SystemExit(f"FAIL: unexpected status or key schema for {TABLE}")
    print(f"PASS: DynamoDB table {TABLE} is active with the lease_name key")


if __name__ == "__main__":
    main(sys.argv[1])
