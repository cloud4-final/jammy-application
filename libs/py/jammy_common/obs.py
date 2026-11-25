"""OpenTelemetry and Prometheus setup for one service process."""

from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.metrics import Histogram, Meter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.view import (
    DropAggregation,
    ExplicitBucketHistogramAggregation,
    View,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prometheus_client import CollectorRegistry

from . import lifecycle
from .config import CommonConfig, env

HTTP_BUCKETS = (
    0.001,
    0.0025,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
)
RESOURCE_LABELS = frozenset({"service", "instance", "site"})
METRIC_LABELS = RESOURCE_LABELS | {"result", "reason", "kind", "profile"}
HTTP_METRIC_LABELS = frozenset(
    {
        "http_request_method",
        "http.request.method",
        "http_route",
        "http.route",
        "http_response_status_code",
        "http.response.status_code",
    }
)
REGISTRY = CollectorRegistry()


class _FallbackView(View):
    # Matching SDK views produce separate streams, so the attribute-only view
    # must skip instruments that already have an explicit bucket view.
    def __init__(self, *, preferred: View, **kwargs):
        super().__init__(**kwargs)
        self._preferred = preferred

    def _match(self, instrument) -> bool:
        return super()._match(instrument) and not self._preferred._match(instrument)


def _resource(service: str, cfg: CommonConfig) -> Resource:
    return Resource.create(
        {
            "service.name": service,
            "service.instance.id": cfg.instance_id,
            "service": service,
            "instance": cfg.instance_id,
            "site": cfg.site_id,
        }
    )


def setup_tracing(service: str, cfg: CommonConfig) -> None:
    tracer_provider = TracerProvider(resource=_resource(service, cfg), shutdown_on_exit=False)
    lifecycle.on_shutdown(tracer_provider.shutdown)
    if endpoint := env("OTEL_EXPORTER_OTLP_ENDPOINT"):
        tracer_provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint))
        )
    trace.set_tracer_provider(tracer_provider)


def setup(service: str, cfg: CommonConfig) -> Meter:
    resource = _resource(service, cfg)
    reader = PrometheusMetricReader(
        registry=REGISTRY,
        disable_target_info=True,
        scope_info_enabled=False,
        resource_attribute_filter=RESOURCE_LABELS.__contains__,
    )
    duration_view = View(
        instrument_name="*_duration",
        instrument_type=Histogram,
        instrument_unit="s",
        attribute_keys=METRIC_LABELS,
        aggregation=ExplicitBucketHistogramAggregation(boundaries=HTTP_BUCKETS),
    )
    http_duration_view = View(
        instrument_name="http.server.request.duration",
        instrument_type=Histogram,
        instrument_unit="s",
        attribute_keys=METRIC_LABELS | HTTP_METRIC_LABELS,
        aggregation=ExplicitBucketHistogramAggregation(boundaries=HTTP_BUCKETS),
    )
    provider = MeterProvider(
        resource=resource,
        shutdown_on_exit=False,
        metric_readers=[reader],
        views=[
            View(instrument_name="*", aggregation=DropAggregation()),
            _FallbackView(
                preferred=duration_view, instrument_name="jammy_*", attribute_keys=METRIC_LABELS
            ),
            _FallbackView(
                preferred=http_duration_view,
                instrument_name="http.server.request.duration",
                attribute_keys=METRIC_LABELS | HTTP_METRIC_LABELS,
            ),
            duration_view,
            http_duration_view,
        ],
    )
    lifecycle.on_shutdown(lambda: provider.shutdown(timeout_millis=cfg.shutdown_timeout_s * 1000))
    metrics.set_meter_provider(provider)

    setup_tracing(service, cfg)
    return provider.get_meter(service)
