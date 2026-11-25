package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cloud4-final/jammy-5th/libs/go/codec"
	"github.com/cloud4-final/jammy-5th/libs/go/gate"
	pb "github.com/cloud4-final/jammy-5th/libs/go/pb/jammyv1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const route = "/v1/batches"

type Config struct {
	Instance            string
	MaxBody, MaxDecoded int64
	Inflight            int
	Gate                gate.Config
}

type Batch struct {
	Envelope   *pb.EventBatch
	Wire       []byte
	Media      string
	Encoding   string
	IngestID   string
	ReceivedAt time.Time
	Result     gate.Result
}

type Sink interface {
	Store(context.Context, Batch) (duplicate bool, err error)
}

type Backend interface {
	Sink
	Prepare(context.Context) error
	Check(context.Context) bool
}

type Receipt struct {
	BatchID   string         `json:"batch_id"`
	Accepted  int            `json:"accepted"`
	Rejected  int            `json:"rejected"`
	Reasons   map[string]int `json:"reasons"`
	Duplicate bool           `json:"duplicate"`
}

type Intake struct {
	config            Config
	sink              Sink
	handler           http.Handler
	slots             chan struct{}
	inflight          atomic.Int64
	ready, draining   atomic.Bool
	events, bytes     metric.Int64Counter
	duration, storage metric.Float64Histogram
}

func New(cfg Config, sink Sink, meter metric.Meter) (*Intake, error) {
	i := &Intake{config: cfg, sink: sink, slots: make(chan struct{}, cfg.Inflight)}
	var eventsErr, bytesErr, durationErr, storageErr, inflightErr, limitErr error
	i.events, eventsErr = meter.Int64Counter("jammy_ingest_events")
	i.bytes, bytesErr = meter.Int64Counter("jammy_ingest_bytes")
	i.duration, durationErr = meter.Float64Histogram("http.server.request.duration", metric.WithUnit("s"))
	i.storage, storageErr = meter.Float64Histogram("jammy_ingest_store_duration", metric.WithUnit("s"))
	_, inflightErr = meter.Int64ObservableGauge(
		"jammy_ingest_inflight_requests",
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(i.inflight.Load())
			return nil
		}),
	)
	_, limitErr = meter.Int64ObservableGauge(
		"jammy_ingest_inflight_limit",
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(cfg.Inflight))
			return nil
		}),
	)
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+route, i.submit)
	i.handler = mux
	return i, errors.Join(eventsErr, bytesErr, durationErr, storageErr, inflightErr, limitErr)
}

func (i *Intake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	i.handler.ServeHTTP(w, r)
}

func (i *Intake) SetReady(value bool) { i.ready.Store(value) }
func (i *Intake) Drain()              { i.draining.Store(true) }
func (i *Intake) Ready(context.Context) bool {
	return i.ready.Load() && !i.draining.Load()
}

func (i *Intake) Maintain(ctx context.Context, sink Backend) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var refresh time.Time
	for ctx.Err() == nil {
		check, cancel := context.WithTimeout(ctx, time.Second)
		ready := true
		if !time.Now().Before(refresh) {
			ready = sink.Prepare(check) == nil
			if ready {
				refresh = time.Now().Add(10 * time.Minute)
			}
		}
		ready = ready && sink.Check(check)
		cancel()
		if !ready {
			refresh = time.Time{}
			if i.ready.Load() {
				slog.Warn("Storage dependency unavailable")
			}
		}
		i.SetReady(ready)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (i *Intake) submit(w http.ResponseWriter, r *http.Request) {
	received := time.Now().UTC()
	ctx := propagation.TraceContext{}.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
	ctx, span := otel.Tracer("ingest").Start(
		ctx,
		"POST "+route,
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			attribute.String("http.request.method", http.MethodPost),
			attribute.String("http.route", route),
		),
	)
	defer span.End()
	status := http.StatusInternalServerError
	defer func() {
		span.SetAttributes(attribute.Int("http.response.status_code", status))
		if status >= http.StatusInternalServerError {
			span.SetStatus(codes.Error, "")
		}
		i.duration.Record(ctx, time.Since(received).Seconds(), metric.WithAttributes(
			attribute.String("http.request.method", http.MethodPost),
			attribute.String("http.route", route),
			attribute.Int("http.response.status_code", status),
		))
	}()
	if i.draining.Load() || !i.ready.Load() {
		// Prevent net/http from draining an unread body before sending the response.
		w.Header().Set("Connection", "close")
		status = unavailable(w)
		return
	}
	select {
	case i.slots <- struct{}{}:
		i.inflight.Add(1)
		defer func() {
			i.inflight.Add(-1)
			<-i.slots
		}()
	default:
		w.Header().Set("Connection", "close")
		status = problem(w, http.StatusTooManyRequests, "too_many_requests", "Too Many Requests", "Ingest is busy", "1")
		return
	}
	status = i.accept(ctx, w, r, received)
}

func (i *Intake) accept(ctx context.Context, w http.ResponseWriter, r *http.Request, received time.Time) int {
	wire, err := io.ReadAll(http.MaxBytesReader(w, r.Body, i.config.MaxBody))
	if err != nil {
		var limit *http.MaxBytesError
		if errors.As(err, &limit) {
			return tooLarge(w, "Request body exceeds the wire size limit")
		}
		return problem(w, http.StatusBadRequest, "bad_request", "Bad Request", "Request body was interrupted", "")
	}
	i.bytes.Add(ctx, int64(len(wire)), metric.WithAttributes(attribute.String("kind", "wire")))
	media, _, _ := strings.Cut(r.Header.Get("Content-Type"), ";")
	media = strings.ToLower(strings.TrimSpace(media))
	encoding := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Encoding")))
	batch, decoded, err := codec.DecodeBatch(wire, media, encoding, i.config.MaxBody, i.config.MaxDecoded)
	if err != nil {
		var failure *codec.HTTPError
		if errors.As(err, &failure) && failure.Status == http.StatusRequestEntityTooLarge {
			return tooLarge(w, failure.Msg)
		}
		return problem(w, http.StatusBadRequest, "bad_request", "Bad Request", err.Error(), "")
	}
	i.bytes.Add(ctx, int64(len(decoded)), metric.WithAttributes(attribute.String("kind", "decompressed")))
	result := gate.Validate(batch, received, i.config.Gate)

	start := time.Now()
	duplicate, err := i.sink.Store(ctx, Batch{
		Envelope: batch, Wire: wire, Media: media, Encoding: encoding, IngestID: i.config.Instance,
		ReceivedAt: received, Result: result,
	})
	i.storage.Record(ctx, time.Since(start).Seconds())
	if err != nil {
		slog.WarnContext(ctx, "Batch storage failed")
		return unavailable(w)
	}

	receipt := Receipt{BatchID: batch.BatchId, Reasons: make(map[string]int), Duplicate: duplicate}
	if !duplicate {
		receipt.Accepted = len(result.Accepted)
		receipt.Rejected = len(result.Rejected)
		for _, rejected := range result.Rejected {
			receipt.Reasons[rejected.Reason]++
		}
		if receipt.Accepted > 0 {
			i.events.Add(ctx, int64(receipt.Accepted), eventLabels("accepted", ""))
		}
		for reason, count := range receipt.Reasons {
			i.events.Add(ctx, int64(count), eventLabels("rejected", reason))
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(receipt)
	return http.StatusAccepted
}

func eventLabels(result, reason string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("result", result), attribute.String("reason", reason))
}

func tooLarge(w http.ResponseWriter, detail string) int {
	return problem(w, http.StatusRequestEntityTooLarge, "payload_too_large", "Payload Too Large", detail, "")
}

func unavailable(w http.ResponseWriter) int {
	return problem(w, http.StatusServiceUnavailable, "unavailable", "Unavailable", "Storage cannot accept the batch", "2")
}

func problem(w http.ResponseWriter, status int, code, title, detail, retry string) int {
	if retry != "" {
		w.Header().Set("Retry-After", retry)
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Type   string `json:"type"`
		Title  string `json:"title"`
		Status int    `json:"status"`
		Detail string `json:"detail"`
	}{"https://jammy.dev/problems/" + code, title, status, detail})
	return status
}
