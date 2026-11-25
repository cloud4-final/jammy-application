import io
import json
import unittest
from contextlib import redirect_stdout
from unittest.mock import Mock, patch

import init_aws
from botocore.exceptions import ConnectTimeoutError, ReadTimeoutError
from botocore.stub import Stubber

TABLE_PARAMS = {"TableName": "jammy-site-lease"}
KEY_SCHEMA = [{"AttributeName": "lease_id", "KeyType": "HASH"}]
CREATE_PARAMS = {
    **TABLE_PARAMS,
    "AttributeDefinitions": [{"AttributeName": "lease_id", "AttributeType": "S"}],
    "KeySchema": KEY_SCHEMA,
    "BillingMode": "PAY_PER_REQUEST",
}


class QueueTests(unittest.TestCase):
    def test_new_queue_uses_matching_dead_letter_queue(self):
        sqs = init_aws.client("sqs", "http://localhost:9324")
        for name, dlq_name, attributes in (
            ("instructions", "instructions-dlq", {}),
            ("instructions.fifo", "instructions-dlq.fifo", {"FifoQueue": "true"}),
        ):
            with self.subTest(name=name), Stubber(sqs) as stub:
                dlq_url = f"http://localhost:9324/000000000000/{dlq_name}"
                dlq_arn = f"arn:aws:sqs:ap-northeast-2:000000000000:{dlq_name}"
                stub.add_response("create_queue", {"QueueUrl": dlq_url}, {
                    "QueueName": dlq_name, "Attributes": attributes,
                })
                stub.add_response("get_queue_attributes", {"Attributes": {"QueueArn": dlq_arn}}, {
                    "QueueUrl": dlq_url, "AttributeNames": ["QueueArn"],
                })
                stub.add_client_error("get_queue_url", "QueueDoesNotExist", expected_params={"QueueName": name})
                stub.add_response("create_queue", {}, {"QueueName": name, "Attributes": {
                    **attributes,
                    "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": 5}),
                }})
                init_aws.create_queue(sqs, name)
                stub.assert_no_pending_responses()


class InitAwsTests(unittest.TestCase):
    def setUp(self):
        self.dynamodb = init_aws.client("dynamodb", "http://localhost:8000")
        self.stub = self.enterContext(Stubber(self.dynamodb))
        sqs = Mock()
        sqs.create_queue.return_value = {"QueueUrl": "http://localhost:9324/000000000000/jammy-instructions-dlq"}
        sqs.get_queue_attributes.return_value = {"Attributes": {"QueueArn": "arn:aws:sqs:ap-northeast-2:000000000000:jammy-instructions-dlq"}}
        sqs.get_queue_url.return_value = {"QueueUrl": "http://localhost:9324/000000000000/jammy-instructions"}
        clients = {"s3": Mock(), "sqs": sqs, "dynamodb": self.dynamodb}
        self.enterContext(patch.object(init_aws, "client", side_effect=lambda service, _: clients[service]))
        self.enterContext(patch.object(init_aws.time, "sleep"))

    def describe_table(self, status):
        self.stub.add_response(
            "describe_table",
            {"Table": {**TABLE_PARAMS, "TableStatus": status, "KeySchema": KEY_SCHEMA}},
            TABLE_PARAMS,
        )

    def assert_init_succeeds(self):
        output = io.StringIO()
        with redirect_stdout(output):
            init_aws.main("init")
        self.assertIn("table jammy-site-lease is active", output.getvalue())
        self.stub.assert_no_pending_responses()

    def test_create_timeout_rechecks_existing_table(self):
        for error_type in (ReadTimeoutError, ConnectTimeoutError):
            with self.subTest(error_type=error_type):
                self.describe_table("CREATING")
                self.describe_table("ACTIVE")
                self.describe_table("ACTIVE")
                error = error_type(endpoint_url="http://localhost:8000")
                with patch.object(self.dynamodb, "create_table", side_effect=error) as create:
                    self.assert_init_succeeds()
                    create.assert_called_once_with(**CREATE_PARAMS)

    def test_create_timeout_retries_when_table_is_missing(self):
        self.stub.add_client_error(
            "describe_table", "ResourceNotFoundException", expected_params=TABLE_PARAMS
        )
        self.describe_table("ACTIVE")
        self.describe_table("ACTIVE")
        error = ReadTimeoutError(endpoint_url="http://localhost:8000")
        with patch.object(self.dynamodb, "create_table", side_effect=[error, {}]) as create:
            self.assert_init_succeeds()
            self.assertEqual(create.call_count, 2)

    def test_init_waits_for_a_new_table_to_become_active(self):
        self.stub.add_response("create_table", {}, CREATE_PARAMS)
        self.describe_table("CREATING")
        self.describe_table("ACTIVE")
        self.describe_table("ACTIVE")
        self.assert_init_succeeds()

    def test_init_accepts_a_concurrent_create(self):
        self.stub.add_client_error(
            "create_table", "ResourceInUseException", http_status_code=400,
            expected_params=CREATE_PARAMS,
        )
        self.describe_table("CREATING")
        self.describe_table("ACTIVE")
        self.describe_table("ACTIVE")
        self.assert_init_succeeds()

    def test_init_uses_configured_table_and_partition_key(self):
        with patch.dict("os.environ", {"LEASE_TABLE": "custom-lease", "LEASE_KEY_NAME": "lease_name"}):
            self.stub.add_response(
                "create_table", {}, CREATE_PARAMS | {
                    "TableName": "custom-lease",
                    "AttributeDefinitions": [{"AttributeName": "lease_name", "AttributeType": "S"}],
                    "KeySchema": [{"AttributeName": "lease_name", "KeyType": "HASH"}],
                },
            )
            for _ in range(2):
                self.stub.add_response(
                    "describe_table",
                    {"Table": {
                        "TableName": "custom-lease",
                        "TableStatus": "ACTIVE",
                        "KeySchema": [{"AttributeName": "lease_name", "KeyType": "HASH"}],
                    }},
                    {"TableName": "custom-lease"},
                )
            init_aws.main("init")
            self.stub.assert_no_pending_responses()
