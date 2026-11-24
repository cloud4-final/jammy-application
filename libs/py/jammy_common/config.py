"""Common environment settings from contracts/ENV.md."""

import os
import re
import socket
from collections.abc import Callable
from dataclasses import dataclass
from functools import wraps
from pathlib import Path


class ConfigError(ValueError):
    """An environment setting is missing or invalid."""


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


def _env_int(name: str, default: int) -> int:
    value = env(name, str(default))
    if re.fullmatch(r"[0-9]+", value):
        try:
            return int(value)
        except ValueError:
            pass
    raise ConfigError(f"{name} must be a non-negative integer")


def _port(name: str, default: int) -> int:
    port = _env_int(name, default)
    if not 1 <= port <= 65535:
        raise ConfigError(f"{name} must be between 1 and 65535")
    return port


def positive_int(name: str, default: int) -> int:
    value = _env_int(name, default)
    if value < 1:
        raise ConfigError(f"{name} must be positive")
    return value


def positive_size(name: str, default: str) -> int:
    match = re.fullmatch(r"([0-9]+)(KiB|MiB|GiB)?", env(name, default))
    if not match:
        raise ConfigError(f"{name} must be a byte size")
    size = int(match[1]) * {None: 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3}[match[2]]
    if not size:
        raise ConfigError(f"{name} must be positive")
    return size


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
        pipeline_mode = env("PIPELINE_MODE", "jetstream")
        if pipeline_mode not in {"jetstream", "direct-db"}:
            raise ConfigError("PIPELINE_MODE must be jetstream or direct-db")

        return cls(
            site_id=env("SITE_ID", required=True),
            instance_id=env("INSTANCE_ID", socket.gethostname(), required=True),
            http_port=_port("HTTP_PORT", 8080),
            admin_port=_port("ADMIN_PORT", 9090),
            log_level=env("LOG_LEVEL", "info").lower(),
            shutdown_timeout_s=_env_int("SHUTDOWN_TIMEOUT_SECONDS", 25),
            pipeline_mode=pipeline_mode,
        )
