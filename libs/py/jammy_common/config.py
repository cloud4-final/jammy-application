"""Common environment settings from contracts/ENV.md."""

import os
import re
import socket
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from functools import wraps
from math import isfinite
from pathlib import Path
from urllib.parse import urlsplit

import psycopg
from psycopg.conninfo import conninfo_to_dict

_MAX_DURATION_SECONDS = (2**63 - 1) // 1_000_000_000


class ConfigError(ValueError):
    """An environment setting is missing or invalid."""


def parse_log_level(value: str) -> int:
    # Go's slog accepts a case-insensitive base level and a signed decimal offset.
    match = re.fullmatch(r"(debug|info|warn|error)([+-][0-9]+)?", value, re.IGNORECASE | re.ASCII)
    if match is None:
        raise ConfigError("invalid LOG_LEVEL")
    base, offset = match.groups()
    try:
        delta = int(offset or "0")
    except ValueError:
        raise ConfigError("invalid LOG_LEVEL") from None
    if not -(1 << 63) <= delta < (1 << 63):
        raise ConfigError("invalid LOG_LEVEL")
    level = {"debug": -4, "info": 0, "warn": 4, "error": 8}[base.lower()] + delta
    # Map slog's four-unit intervals to the standard Python ten-unit intervals.
    return 20 + (5 * level + 1) // 2


def config_error_exit(main: Callable[[], None]) -> Callable[[], None]:
    """Print startup configuration errors and exit with status 1."""
    @wraps(main)
    def entrypoint() -> None:
        try:
            main()
        except ConfigError as exc:
            raise SystemExit(str(exc)) from None

    return entrypoint


def env(name: str, default: str | None = None, required: bool = False) -> str:
    file_name = f"{name}_FILE"
    if file_path := os.environ.get(file_name):
        try:
            value = Path(file_path).read_text(encoding="utf-8").rstrip("\r\n")
        except OSError, UnicodeError:
            raise ConfigError(f"Cannot read {file_name}") from None
    else:
        value = os.environ.get(name, default)

    if required and not value:
        raise ConfigError(f"{name} is required")
    return value if value is not None else ""


def _parse_int(name: str, value: str) -> int:
    if re.fullmatch(r"[0-9]+", value):
        try:
            return int(value)
        except ValueError:
            pass
    raise ConfigError(f"{name} must be a non-negative integer")


def env_int(name: str, default: int) -> int:
    return _parse_int(name, env(name, str(default)))


def _port(name: str, default: int) -> int:
    port = env_int(name, default)
    if not 1 <= port <= 65535:
        raise ConfigError(f"{name} must be between 1 and 65535")
    return port


def positive_int(name: str, default: int) -> int:
    value = env_int(name, default)
    if value < 1:
        raise ConfigError(f"{name} must be positive")
    return value


def parse_duration(name: str, value: str, *, units_hint: str = "in s, m or h") -> int:
    """Parse integer seconds or an integer with a single duration unit."""
    multiplier = 1
    if name.endswith("_SECONDS"):
        seconds = _parse_int(name, value)
    else:
        match = re.fullmatch(r"([0-9]+)([smh])", value)
        if not match:
            raise ConfigError(f"{name} must be a duration {units_hint}")
        seconds = _parse_int(name, match[1])
        multiplier = {"s": 1, "m": 60, "h": 3600}[match[2]]
    duration = seconds * multiplier
    if duration > _MAX_DURATION_SECONDS:
        raise ConfigError(f"{name} exceeds the maximum supported duration")
    return duration


def env_duration(name: str, default: str, *, units_hint: str = "in s, m or h") -> int:
    return parse_duration(name, env(name, default), units_hint=units_hint)


def parse_size(name: str, value: str, *, signed_64_bit: bool = True) -> int:
    """Parse bytes with an optional binary unit suffix."""
    match = re.fullmatch(r"([0-9]+)(KiB|MiB|GiB)?", value)
    description = "valid signed 64-bit byte size" if signed_64_bit else "byte size"
    if not match:
        raise ConfigError(f"{name} must be a {description}")
    try:
        amount = int(match[1])
    except ValueError:
        if not signed_64_bit:
            raise
        raise ConfigError(f"{name} must fit in a signed 64-bit byte size") from None
    size = amount * {None: 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3}[match[2]]
    if signed_64_bit and size > 2**63 - 1:
        raise ConfigError(f"{name} must be a {description}")
    return size


def positive_size(name: str, default: str) -> int:
    size = parse_size(name, env(name, default))
    if not size:
        raise ConfigError(f"{name} must be positive")
    return size


def env_ratio[Number: (float, Decimal)](
    name: str, default: str, number_type: type[Number]
) -> Number:
    """Read a ratio in (0, 1], preserving the caller's numeric representation."""
    message = (
        f"{name} must be greater than 0 and at most 1"
        if number_type is Decimal
        else f"{name} must be in (0, 1]"
    )
    try:
        ratio = number_type(env(name, default))
    except ConfigError:
        if number_type is Decimal:
            raise
        raise ConfigError(message) from None
    except (InvalidOperation, ValueError):
        raise ConfigError(message) from None
    finite = ratio.is_finite() if isinstance(ratio, Decimal) else isfinite(ratio)
    if not finite or not 0 < ratio <= 1:
        raise ConfigError(message)
    return ratio


def parse_list(name: str, value: str, *, unique: bool = False) -> list[str]:
    items = value.split(",")
    if any(not item or any(char.isspace() for char in item) for item in items) or (
        unique and len(set(items)) != len(items)
    ):
        raise ConfigError(f"{name} must be a list without whitespace")
    return items


def parse_bool(name: str, value: str) -> bool:
    if value not in {"true", "false"}:
        raise ConfigError(f"{name} must be true or false")
    return value == "true"


_NATS_SCHEMES = frozenset({"nats", "tls", "ws", "wss"})
_NATS_WS_SCHEMES = frozenset({"ws", "wss"})


def validate_http_endpoint(name: str, value: str) -> str:
    try:
        parsed = urlsplit(value)
        _ = parsed.port
    except ValueError:
        raise ConfigError(f"invalid {name}") from None
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or any(ch.isspace() for ch in parsed.netloc)
    ):
        raise ConfigError(f"invalid {name}")
    return value


def validate_database_url(value: str) -> str:
    """Validate a PostgreSQL DSN or conninfo string without connecting."""
    try:
        parsed = conninfo_to_dict(value)
    except (psycopg.Error, ValueError):
        raise ConfigError("invalid DATABASE_URL") from None
    if not parsed:
        raise ConfigError("invalid DATABASE_URL")
    if port_spec := parsed.get("port"):
        for raw_port in port_spec.split(","):
            try:
                port = _parse_int("DATABASE_URL port", raw_port)
            except ConfigError:
                raise ConfigError("invalid DATABASE_URL") from None
            if not 1 <= port <= 65535:
                raise ConfigError("invalid DATABASE_URL")
    return value


def validate_nats_url(value: str) -> str:
    """Validate one or more comma-separated NATS endpoint URLs."""
    schemes: set[str] = set()
    for raw_part in value.split(","):
        part = raw_part.strip()
        if not part:
            raise ConfigError("invalid NATS_URL")
        try:
            parsed = urlsplit(part)
            host = parsed.hostname
            port = parsed.port
        except ValueError:
            raise ConfigError("invalid NATS_URL") from None
        scheme = parsed.scheme.lower()
        if scheme not in _NATS_SCHEMES or not host or port == 0 or any(ch.isspace() for ch in host):
            raise ConfigError("invalid NATS_URL")
        schemes.add(scheme)
    if schemes & _NATS_WS_SCHEMES and schemes - _NATS_WS_SCHEMES:
        raise ConfigError("invalid NATS_URL")
    return value


@dataclass(frozen=True, slots=True)
class CommonConfig:
    site_id: str
    instance_id: str
    http_port: int
    admin_port: int
    log_level: str
    shutdown_timeout_s: int
    pipeline_mode: str

    @classmethod
    def from_env(cls) -> CommonConfig:
        log_level = env("LOG_LEVEL", "info").lower()
        parse_log_level(log_level)
        pipeline_mode = env("PIPELINE_MODE", "jetstream")
        if pipeline_mode not in {"jetstream", "direct-db"}:
            raise ConfigError("PIPELINE_MODE must be jetstream or direct-db")

        return cls(
            site_id=env("SITE_ID", required=True),
            instance_id=env("INSTANCE_ID", socket.gethostname(), required=True),
            http_port=_port("HTTP_PORT", 8080),
            admin_port=_port("ADMIN_PORT", 9090),
            log_level=log_level,
            shutdown_timeout_s=env_duration("SHUTDOWN_TIMEOUT_SECONDS", "25"),
            pipeline_mode=pipeline_mode,
        )
