package logx

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"slices"

	"github.com/cloud4-final/jammy-application/libs/go/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/trace"
)

func Setup(service string, cfg config.Common) error {
	logger, err := New(os.Stdout, service, cfg)
	if err != nil {
		return err
	}
	slog.SetDefault(logger)
	return nil
}

func New(w io.Writer, service string, cfg config.Common) (*slog.Logger, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
		return nil, errors.New("invalid LOG_LEVEL")
	}
	json := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(groups []string, attr slog.Attr) slog.Attr {
			if len(groups) == 0 && attr.Key == slog.TimeKey {
				attr.Key = "ts"
			}
			return attr
		},
	}).WithAttrs([]slog.Attr{
		slog.String("service", service), slog.String("instance", cfg.InstanceID), slog.String("site", cfg.SiteID),
	})
	return slog.New(handler{json: json, otlp: otel.Logger(service)}), nil
}

type handler struct {
	json   slog.Handler
	otlp   otellog.Logger
	group  string
	scopes []scope
	attrs  []attribute.KeyValue
}

type scope struct {
	group string
	attrs []slog.Attr
}

func (h handler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.json.Enabled(ctx, level)
}

func (h handler) Handle(ctx context.Context, record slog.Record) error {
	span := trace.SpanContextFromContext(ctx)
	traceID, spanID := "", ""
	if span.IsValid() {
		traceID = span.TraceID().String()
		spanID = span.SpanID().String()
	}
	json := h.json.WithAttrs([]slog.Attr{slog.String("trace_id", traceID), slog.String("span_id", spanID)})
	for _, scope := range h.scopes {
		if scope.group != "" {
			json = json.WithGroup(scope.group)
		} else {
			json = json.WithAttrs(scope.attrs)
		}
	}

	severity := otellog.SeverityInfo + otellog.Severity(record.Level)
	severity = max(otellog.SeverityTrace, min(otellog.SeverityFatal4, severity))
	if h.otlp.Enabled(ctx, otellog.EnabledParameters{Severity: severity}) {
		var event otellog.Record
		event.SetTimestamp(record.Time)
		event.SetSeverity(severity)
		event.SetSeverityText(record.Level.String())
		event.SetBody(attribute.StringValue(record.Message))
		event.AddAttributes(h.attrs...)
		record.Attrs(func(attr slog.Attr) bool {
			event.AddAttributes(logAttrs(nil, h.group, attr)...)
			return true
		})
		h.otlp.Emit(ctx, event)
	}
	return json.Handle(ctx, record)
}

func (h handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	h.scopes = append(slices.Clone(h.scopes), scope{attrs: slices.Clone(attrs)})
	h.attrs = slices.Clone(h.attrs)
	for _, attr := range attrs {
		h.attrs = logAttrs(h.attrs, h.group, attr)
	}
	return h
}

func (h handler) WithGroup(name string) slog.Handler {
	if name != "" {
		h.scopes = append(slices.Clone(h.scopes), scope{group: name})
		h.group += name + "."
	}
	return h
}

func logAttrs(attrs []attribute.KeyValue, prefix string, attr slog.Attr) []attribute.KeyValue {
	value := attr.Value.Resolve()
	key := prefix + attr.Key
	switch value.Kind() {
	case slog.KindGroup:
		if attr.Key != "" {
			prefix = key + "."
		}
		for _, member := range value.Group() {
			attrs = logAttrs(attrs, prefix, member)
		}
		return attrs
	case slog.KindBool:
		return append(attrs, attribute.Bool(key, value.Bool()))
	case slog.KindInt64:
		return append(attrs, attribute.Int64(key, value.Int64()))
	case slog.KindFloat64:
		return append(attrs, attribute.Float64(key, value.Float64()))
	default:
		return append(attrs, attribute.String(key, value.String()))
	}
}
