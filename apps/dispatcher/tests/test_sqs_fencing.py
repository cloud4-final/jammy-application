import asyncio
import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, Mock

from dispatcher.lease import LeaseState
from dispatcher.sqs_drain import SQSDrain


def test_fifo_drain_only_receives_as_holder_and_deletes_after_committing_expired():
    async def exercise():
        trace = []
        client = Mock()
        body = json.dumps({
            "instruction_id": "expired",
            "target_reduction_kw": "12.500",
            "valid_from": "2026-11-23T01:00:00Z",
            "valid_until": "2026-11-23T02:00:00Z",
        })
        client.receive_message.return_value = {
            "Messages": [{"Body": body, "ReceiptHandle": "receipt"}]
        }
        client.delete_message.side_effect = lambda **kwargs: trace.append("delete")
        conn = AsyncMock()
        conn.execute.return_value.fetchone.return_value = ("expired",)
        active = AsyncMock(return_value=LeaseState(False, 1))
        url = "https://queue.example/instructions.fifo"
        drain = SQSDrain(client, url, Mock())

        @asynccontextmanager
        async def transaction():
            yield conn
            trace.append("commit")

        assert await drain.drain_once(transaction, active)
        client.receive_message.assert_not_called()

        active.side_effect = [LeaseState(True, 1), LeaseState(False, 1)]
        assert await drain.drain_once(transaction, active)
        conn.execute.assert_not_awaited()
        client.delete_message.assert_not_called()

        active.side_effect = None
        active.return_value = LeaseState(True, 1)
        assert await drain.drain_once(transaction, active)
        assert trace == ["commit", "delete"]
        sql, params = conn.execute.await_args.args
        assert "expired" in sql and "valid_until" in sql
        assert params[0] == "expired"
        client.receive_message.assert_called_with(
            QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=0,
        )
        client.delete_message.assert_called_once_with(QueueUrl=url, ReceiptHandle="receipt")

        active.side_effect = [LeaseState(True, 1), LeaseState(True, 1), LeaseState(False, 1)]
        client.delete_message.reset_mock()
        assert await drain.drain_once(transaction, active)
        assert trace == ["commit", "delete", "commit"]
        client.delete_message.assert_not_called()

    asyncio.run(exercise())


def test_sqs_drain_treats_nul_bytes_as_invalid_without_querying_postgres():
    async def exercise():
        for payload in (
            {
                "instruction_id": "bad\0id",
                "target_reduction_kw": "12.500",
                "valid_from": "2026-11-23T01:00:00Z",
                "valid_until": "2026-11-23T02:00:00Z",
            },
            {
                "instruction_id": "ok-id",
                "target_reduction_kw": "12.500",
                "valid_from": "2026-11-23T01:00:00Z",
                "valid_until": "2026-11-23T02:00:00Z",
                "routing_keys": ["solar\0-1"],
            },
        ):
            client = Mock()
            client.receive_message.return_value = {
                "Messages": [{"Body": json.dumps(payload), "ReceiptHandle": "receipt"}]
            }
            conn = AsyncMock()
            counter = Mock()
            drain = SQSDrain(client, "https://queue.example/instructions", counter)

            @asynccontextmanager
            async def transaction(db_conn=conn):
                yield db_conn

            assert await drain.drain_once(transaction, AsyncMock(return_value=LeaseState(True, 1)))
            conn.execute.assert_not_awaited()
            client.delete_message.assert_not_called()
            counter.add.assert_called_once_with(1, {"result": "invalid"})

    asyncio.run(exercise())
