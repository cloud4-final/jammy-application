"""Authenticate instructions over the exact HTTP request bytes."""

import hashlib
import hmac
import json
import re
import time
from dataclasses import dataclass
from pathlib import Path

from fastapi import Request
from jammy_common.config import ConfigError, env_int

SIGNATURE = re.compile(r"[0-9a-f]{64}")
TIMESTAMP = re.compile(r"[0-9]+")


def sign(secret: bytes, ts: int, method: str, path: str, body: bytes | bytearray) -> str:
    digest = hashlib.sha256(body).hexdigest()
    return hmac.digest(secret, f"{ts}\n{method}\n{path}\n{digest}".encode(), "sha256").hex()


@dataclass(frozen=True, slots=True)
class SignedRequest:
    secret: bytes
    timestamp: int
    signature: str

    def verify(self, request: Request, body: bytearray) -> bool:
        raw_path = request.scope.get("raw_path")
        path = raw_path.decode("ascii") if raw_path is not None else request.url.path
        expected = sign(self.secret, self.timestamp, request.method, path, body)
        return hmac.compare_digest(self.signature, expected)


@dataclass(frozen=True)
class HMACAuth:
    keys: dict[str, bytes]
    max_skew: int

    @classmethod
    def from_file(cls, filename: str) -> HMACAuth:
        try:
            keys = json.loads(Path(filename).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            raise ConfigError("Cannot load HMAC_KEYS_FILE") from None
        if not isinstance(keys, dict) or not keys or not all(
            key_id and isinstance(secret, str) and secret for key_id, secret in keys.items()
        ):
            raise ConfigError("HMAC_KEYS_FILE must contain a non-empty key-to-secret mapping")
        max_skew = env_int("HMAC_MAX_SKEW_SECONDS", 300)
        if max_skew < 1:
            raise ConfigError("HMAC_MAX_SKEW_SECONDS must be positive")
        return cls({key_id: secret.encode() for key_id, secret in keys.items()}, max_skew)

    def validate_headers(self, request: Request) -> SignedRequest | str:
        key_id = request.headers.get("X-Jammy-Key-Id", "")
        timestamp = request.headers.get("X-Jammy-Timestamp", "")
        signature = request.headers.get("X-Jammy-Signature", "")
        if not (key_id and TIMESTAMP.fullmatch(timestamp) and SIGNATURE.fullmatch(signature)):
            return "bad_signature"
        secret = self.keys.get(key_id)
        if secret is None:
            return "unknown_key"
        try:
            ts = int(timestamp)
        except ValueError:
            return "bad_signature"
        if abs(int(time.time()) - ts) > self.max_skew:
            return "stale_timestamp"
        return SignedRequest(secret, ts, signature)
