package logx

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/cloud4-final/jammy-application/libs/go/config"
	"go.opentelemetry.io/otel/trace"
)

func TestJSONLogIncludesTraceContext(t *testing.T) {
	var output bytes.Buffer
	logger, err := New(&output, "ingest", config.Common{SiteID: "site-a", InstanceID: "ingest-a1", LogLevel: "info"})
	if err != nil {
		t.Fatal(err)
	}
	contextWithSpan := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2},
	}))
	logger.Debug("filtered")
	logger.InfoContext(contextWithSpan, "processed")
	var row map[string]string
	if err := json.Unmarshal(output.Bytes(), &row); err != nil {
		t.Fatal(err)
	}
	if row["ts"] == "" || row["level"] != "INFO" || row["msg"] != "processed" ||
		row["service"] != "ingest" || row["site"] != "site-a" || row["instance"] != "ingest-a1" ||
		row["trace_id"] != "01000000000000000000000000000000" || row["span_id"] != "0200000000000000" {
		t.Fatalf("unexpected log record: %v", row)
	}
}

func TestGroupedLogKeepsTraceFieldsAtTopLevel(t *testing.T) {
	var output bytes.Buffer
	logger, err := New(&output, "ingest", config.Common{LogLevel: "info"})
	if err != nil {
		t.Fatal(err)
	}
	logger.WithGroup("request").With("code", 202).Info("stored")
	var row map[string]json.RawMessage
	if err := json.Unmarshal(output.Bytes(), &row); err != nil {
		t.Fatal(err)
	}
	if string(row["trace_id"]) != `""` || string(row["span_id"]) != `""` || string(row["request"]) != `{"code":202}` {
		t.Fatalf("unexpected log record: %s", output.String())
	}
}
