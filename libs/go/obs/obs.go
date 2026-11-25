package obs

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"

	"github.com/cloud4-final/jammy-5th/libs/go/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/exemplar"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

var durationBuckets = []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
var registry atomic.Pointer[prometheus.Registry]
var emptyRegistry = prometheus.NewRegistry()

// Setup installs the process-wide telemetry providers and Prometheus registry.
func Setup(service string, c config.Common) (metric.Meter, func(context.Context) error, error) {
	ctx := context.Background()
	res, err := resource.New(ctx, resource.WithFromEnv(), resource.WithAttributes(
		attribute.String("service.name", service),
		attribute.String("service.instance.id", c.InstanceID),
		attribute.String("service", service),
		attribute.String("instance", c.InstanceID),
		attribute.String("site", c.SiteID),
	))
	if errors.Is(err, resource.ErrPartialResource) {
		return nil, nil, errors.New("invalid OTEL_RESOURCE_ATTRIBUTES")
	}
	if err != nil {
		return nil, nil, err
	}

	endpoint, err := config.Lookup("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	if err != nil {
		return nil, nil, err
	}
	reg := prometheus.NewRegistry()
	reader, err := otelprom.New(
		otelprom.WithRegisterer(reg),
		otelprom.WithoutTargetInfo(),
		otelprom.WithoutScopeInfo(),
		otelprom.WithResourceAsConstantLabels(resourceLabel),
	)
	if err != nil {
		return nil, nil, err
	}
	metrics := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader), sdkmetric.WithResource(res), sdkmetric.WithView(metricView),
		// Exemplars retain attributes removed by the metric view.
		sdkmetric.WithExemplarFilter(exemplar.AlwaysOffFilter),
	)
	shutdown := metrics.Shutdown
	if endpoint != "" {
		traces, logs, err := exporters(ctx, endpoint, res)
		if err != nil {
			return nil, nil, errors.Join(err, metrics.Shutdown(ctx))
		}
		otel.SetTracerProvider(traces)
		otel.SetLoggerProvider(logs)
		shutdown = func(ctx context.Context) error {
			return errors.Join(metrics.Shutdown(ctx), traces.Shutdown(ctx), logs.Shutdown(ctx))
		}
	}
	registry.Store(reg)
	otel.SetMeterProvider(metrics)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	return metrics.Meter(service), shutdown, nil
}

func exporters(ctx context.Context, endpoint string, res *resource.Resource) (*sdktrace.TracerProvider, *sdklog.LoggerProvider, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return nil, nil, errors.New("invalid OTEL_EXPORTER_OTLP_ENDPOINT")
	}
	traces, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpointURL(endpoint))
	if err != nil {
		return nil, nil, err
	}
	logs, err := otlploggrpc.New(ctx, otlploggrpc.WithEndpointURL(endpoint))
	if err != nil {
		return nil, nil, errors.Join(err, traces.Shutdown(ctx))
	}
	return sdktrace.NewTracerProvider(sdktrace.WithResource(res), sdktrace.WithBatcher(traces)),
		sdklog.NewLoggerProvider(sdklog.WithResource(res), sdklog.WithProcessor(sdklog.NewBatchProcessor(logs))), nil
}

func resourceLabel(kv attribute.KeyValue) bool {
	switch kv.Key {
	case "service", "instance", "site":
		return true
	default:
		return false
	}
}

func applicationLabel(kv attribute.KeyValue) bool {
	switch kv.Key {
	case "result", "reason", "kind", "profile":
		return true
	default:
		return false
	}
}

func httpLabel(kv attribute.KeyValue) bool {
	switch kv.Key {
	case "http.request.method", "http.route", "http.response.status_code":
		return true
	default:
		return applicationLabel(kv)
	}
}

func metricView(instrument sdkmetric.Instrument) (sdkmetric.Stream, bool) {
	stream := sdkmetric.Stream{
		Name: instrument.Name, Description: instrument.Description, Unit: instrument.Unit,
		AttributeFilter: applicationLabel,
	}
	if instrument.Kind == sdkmetric.InstrumentKindHistogram {
		isHTTPDuration := instrument.Name == "http.server.request.duration"
		if isHTTPDuration {
			stream.AttributeFilter = httpLabel
		}
		if instrument.Unit == "s" && (isHTTPDuration || strings.HasSuffix(instrument.Name, "_duration")) {
			stream.Aggregation = sdkmetric.AggregationExplicitBucketHistogram{Boundaries: durationBuckets}
		}
	}
	return stream, true
}

func MetricsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reg := registry.Load()
		if reg == nil {
			reg = emptyRegistry
		}
		promhttp.HandlerFor(reg, promhttp.HandlerOpts{EnableOpenMetrics: true}).ServeHTTP(w, r)
	})
}
