"""Commit SQS instructions before acknowledging their queue receipts."""

import asyncio
import hashlib
import logging

from botocore.exceptions import BotoCoreError, ClientError
from instruction_api.models import Instruction
from psycopg import Error
from pydantic import ValidationError

logger = logging.getLogger(__name__)


class SQSDrain:
    def __init__(self, client, url, counter):
        self.client, self.url, self.counter = client, url, counter

    async def ready(self) -> bool:
        try:
            async with asyncio.timeout(1):
                await asyncio.to_thread(
                    self.client.get_queue_attributes,
                    QueueUrl=self.url,
                    AttributeNames=["QueueArn"],
                )
        except TimeoutError:
            return False
        return True

    async def drain_once(self, conn, active=None) -> bool:
        try:
            response = await asyncio.to_thread(
                self.client.receive_message,
                QueueUrl=self.url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=0,
            )
        except (BotoCoreError, ClientError) as error:
            self._count_error(error)
            return False

        succeeded = True
        for message in response.get("Messages", []):
            if active is not None and not (await active()).active:
                break
            try:
                result = await self._insert(conn, message["Body"].encode())
                if result in {"inserted", "duplicate"}:
                    await asyncio.to_thread(
                        self.client.delete_message,
                        QueueUrl=self.url,
                        ReceiptHandle=message["ReceiptHandle"],
                    )
                self.counter.add(1, {"result": result})
            except (BotoCoreError, ClientError, Error) as error:
                self._count_error(error)
                succeeded = False
        return succeeded

    async def _insert(self, conn, body):
        try:
            instruction = Instruction.model_validate_json(body)
        except ValidationError:
            return "invalid"

        digest = hashlib.sha256(body).hexdigest()
        async with conn.transaction():
            cursor = await conn.execute(
                """
                INSERT INTO jammy.instructions
                    (instruction_id,body,body_sha256,target_reduction_kw,valid_from,valid_until,status,source)
                VALUES (%s,%s,%s,%s,%s,%s,'accepted','sqs')
                ON CONFLICT (instruction_id) DO NOTHING RETURNING instruction_id
                """,
                (
                    instruction.instruction_id,
                    body,
                    digest,
                    instruction.target_reduction_kw,
                    instruction.valid_from,
                    instruction.valid_until,
                ),
            )
            if await cursor.fetchone() is not None:
                return "inserted"
            cursor = await conn.execute(
                "SELECT body_sha256 FROM jammy.instructions WHERE instruction_id = %s",
                (instruction.instruction_id,),
            )
            row = await cursor.fetchone()
            return "duplicate" if row == (digest,) else "conflict"

    def _count_error(self, error):
        self.counter.add(1, {"result": "error"})
        logger.warning("SQS drain failed (%s)", type(error).__name__)
