import io
import unittest
from contextlib import redirect_stdout
from unittest.mock import Mock, patch

from botocore.exceptions import ConnectTimeoutError, ReadTimeoutError
from botocore.stub import Stubber

import init_aws


TABLE_PARAMS = {"TableName": "jammy-site-lease"}
KEY_SCHEMA = [{"AttributeName": "lease_name", "KeyType": "HASH"}]
CREATE_PARAMS = {
    **TABLE_PARAMS,
    "AttributeDefinitions": [{"AttributeName": "lease_name", "AttributeType": "S"}],
    "KeySchema": KEY_SCHEMA,
    "BillingMode": "PAY_PER_REQUEST",
}


class InitAwsTests(unittest.TestCase):
    def setUp(self):
        self.dynamodb = init_aws.client("dynamodb", "http://localhost:8000")
        self.stub = self.enterContext(Stubber(self.dynamodb))
        clients = {"s3": Mock(), "sqs": Mock(), "dynamodb": self.dynamodb}
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
