"""Single-line JSON service logging."""

import json
import logging
import sys
from datetime import UTC, datetime

from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.trace import get_current_span

from . import lifecycle
from .config import CommonConfig, env, parse_log_level
from .obs import _resource


class _JsonFormatter(logging.Formatter):
    def __init__(self, service: str, cfg: CommonConfig) -> None:
        super().__init__()
        self.service = service
        self.cfg = cfg

    def format(self, record: logging.LogRecord) -> str:
        context = get_current_span().get_span_context()
        return json.dumps(
            {
                "ts": datetime.fromtimestamp(record.created, UTC).isoformat(),
                "level": record.levelname.lower(),
                "msg": record.getMessage(),
                "service": self.service,
                "instance": self.cfg.instance_id,
                "site": self.cfg.site_id,
                "trace_id": f"{context.trace_id:032x}" if context.is_valid else "",
                "span_id": f"{context.span_id:016x}" if context.is_valid else "",
            },
            ensure_ascii=False,
        )


class _OTLPHandler(LoggingHandler):
    # Provider shutdown flushes pending records within the lifecycle deadline.
    flushOnClose = False


def setup(service: str, cfg: CommonConfig) -> None:
    formatter = _JsonFormatter(service, cfg)
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)

    logging.basicConfig(level=parse_log_level(cfg.log_level), handlers=[console], force=True)

    if endpoint := env("OTEL_EXPORTER_OTLP_ENDPOINT"):
        provider = LoggerProvider(resource=_resource(service, cfg), shutdown_on_exit=False)
        provider.add_log_record_processor(BatchLogRecordProcessor(OTLPLogExporter(endpoint=endpoint)))
        handler = _OTLPHandler(logger_provider=provider)
        handler.setFormatter(formatter)
        root = logging.getLogger()
        root.addHandler(handler)

        def shutdown() -> None:
            root.removeHandler(handler)
            handler.close()
            provider.shutdown()

        lifecycle.on_shutdown(shutdown)
