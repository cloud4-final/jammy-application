package sinkjs

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/cloud4-final/jammy-5th/apps/ingest-go/internal/server"
	pb "github.com/cloud4-final/jammy-5th/libs/go/pb/jammyv1"
	"github.com/klauspost/compress/zstd"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
)

const protobuf = "application/x-protobuf"

type Sink struct {
	js        jetstream.JetStream
	connected func() bool
	raw       jetstream.StreamConfig
	rejected  jetstream.StreamConfig
}

func New(js jetstream.JetStream, connected func() bool, name string, maxAge time.Duration, maxBytes int64, quarantineAge time.Duration, quarantineBytes int64) *Sink {
	raw := streamConfig(name, "jammy.raw."+name, maxAge, maxBytes)
	rejected := streamConfig("quarantine-"+name, "jammy.quarantine."+name, quarantineAge, quarantineBytes)
	return &Sink{js: js, connected: connected, raw: raw, rejected: rejected}
}

func streamConfig(name, subject string, maxAge time.Duration, maxBytes int64) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name: name, Subjects: []string{subject}, MaxAge: maxAge, MaxBytes: maxBytes,
		Discard: jetstream.DiscardNew, Storage: jetstream.FileStorage,
		Compression: jetstream.NoCompression, Replicas: 1, Duplicates: 2 * time.Minute,
	}
}

func (s *Sink) Prepare(ctx context.Context) error {
	if !s.connected() {
		return errors.New("NATS unavailable")
	}
	for _, cfg := range []jetstream.StreamConfig{s.raw, s.rejected} {
		if _, err := s.js.CreateOrUpdateStream(ctx, cfg); err != nil {
			return err
		}
	}
	return nil
}

func (s *Sink) Check(ctx context.Context) bool {
	if !s.connected() {
		return false
	}
	for _, cfg := range []jetstream.StreamConfig{s.raw, s.rejected} {
		if _, err := s.js.Stream(ctx, cfg.Name); err != nil {
			return false
		}
	}
	return true
}

func (s *Sink) Store(ctx context.Context, req server.Batch) (bool, error) {
	batchID := req.Envelope.BatchId
	received := strconv.FormatInt(req.ReceivedAt.UnixMilli(), 10)
	newMsg := func(subject string) *nats.Msg {
		msg := nats.NewMsg(subject)
		msg.Header.Set("Jammy-Ingest-Id", req.IngestID)
		msg.Header.Set("Jammy-Received-At-Ms", received)
		return msg
	}

	allDuplicate := true
	rejections := req.Result.Rejected
	for index, event := range req.Envelope.Events {
		if len(rejections) == 0 {
			break
		}
		rejected := rejections[0]
		// Validate retains each decoded event pointer in input order.
		if event != rejected.Event {
			continue
		}
		rejections = rejections[1:]
		msg := newMsg(s.rejected.Subjects[0])
		var err error
		msg.Data, err = proto.Marshal(event)
		if err != nil {
			return false, err
		}
		msg.Header.Set("Nats-Msg-Id", fmt.Sprintf("%s:q:%d", batchID, index))
		msg.Header.Set("Jammy-Batch-Id", batchID)
		msg.Header.Set("Jammy-Reason", rejected.Reason)
		ack, err := s.publish(ctx, msg, s.rejected.Name)
		if err != nil {
			return false, err
		}
		allDuplicate = allDuplicate && ack.Duplicate
	}

	if len(req.Result.Accepted) == 0 {
		return allDuplicate, nil
	}
	body, encoding := req.Wire, req.Encoding
	if req.Media != protobuf || len(req.Result.Rejected) > 0 {
		raw, err := proto.Marshal(&pb.EventBatch{
			BatchId: batchID, ProducerId: req.Envelope.ProducerId, Events: req.Result.Accepted,
		})
		if err != nil {
			return false, err
		}
		encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(1)))
		if err != nil {
			return false, err
		}
		defer func() { _ = encoder.Close() }()
		body, encoding = encoder.EncodeAll(raw, nil), "zstd"
	}

	msg := newMsg(s.raw.Subjects[0])
	msg.Data = body
	msg.Header.Set("Nats-Msg-Id", batchID)
	msg.Header.Set("Content-Type", protobuf)
	if encoding != "" {
		msg.Header.Set("Content-Encoding", encoding)
	}
	ack, err := s.publish(ctx, msg, s.raw.Name)
	if err != nil {
		return false, err
	}
	return ack.Duplicate, nil
}

func (s *Sink) publish(ctx context.Context, msg *nats.Msg, stream string) (*jetstream.PubAck, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return s.js.PublishMsg(ctx, msg, jetstream.WithExpectStream(stream))
}
