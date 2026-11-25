// Package gen writes reproducible JAMMY batch corpora and seed SQL.
package gen

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"time"

	pb "github.com/cloud4-final/jammy-application/libs/go/pb/jammyv1"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"
)

const (
	maxEvents    = 10_000
	maxWireBytes = 16 << 20
	maxRawBytes  = 64 << 20
)

type Options struct {
	Profile        string
	EventsPerBatch int
	Batches        int
	Assets         int
	Seed           int64
	Anchor         time.Time // Zero selects a fixed fixture time.
}

type IndexEntry struct {
	File      string `json:"file"`
	BatchID   string `json:"batch_id"`
	Events    int    `json:"events"`
	WireBytes int    `json:"wire_bytes"`
	RawBytes  int    `json:"raw_bytes"`
}

func WriteBatches(dir string, opts Options) (err error) {
	if opts.Batches < 1 {
		return fmt.Errorf("batches must be positive")
	}
	generator, err := NewBatchEncoder(opts, "")
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := generator.Close(); err == nil {
			err = closeErr
		}
	}()

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	files, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	if len(files) != 0 {
		return fmt.Errorf("output directory %q must be empty", dir)
	}

	index := make([]IndexEntry, 0, opts.Batches)
	for range opts.Batches {
		entry, wire, err := generator.Next(nil)
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dir, entry.File), wire, 0o644); err != nil {
			return err
		}
		index = append(index, entry)
	}
	data, err := json.MarshalIndent(index, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "index.json"), append(data, '\n'), 0o644)
}

// BatchEncoder produces the same sequence as WriteBatches without file I/O.
// One goroutine owns each encoder.
type BatchEncoder struct {
	options Options
	prefix  string
	index   int
	source  *rand.ChaCha8
	random  *rand.Rand
	encoder *zstd.Encoder
	raw     []byte
}

// NewBatchEncoder accepts an optional namespace in the batch and event IDs.
// The namespace does not change the random sequence.
func NewBatchEncoder(opts Options, namespace string) (*BatchEncoder, error) {
	switch opts.Profile {
	case "solar", "synth-small", "synth-large":
	default:
		return nil, fmt.Errorf("unsupported profile %q", opts.Profile)
	}
	if opts.EventsPerBatch < 1 || opts.EventsPerBatch > maxEvents || opts.Assets < 1 {
		return nil, fmt.Errorf("assets must be positive and events-per-batch must be in [1, %d]", maxEvents)
	}
	if opts.Anchor.IsZero() {
		opts.Anchor = time.Date(2026, time.November, 23, 0, 0, 0, 0, time.UTC)
	}

	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if err != nil {
		return nil, err
	}
	var seed [32]byte
	binary.LittleEndian.PutUint64(seed[:], uint64(opts.Seed))
	source := rand.NewChaCha8(seed)
	prefix := fmt.Sprintf("%s-%x-%d", opts.Profile, uint64(opts.Seed), opts.Anchor.UnixMilli())
	if namespace != "" {
		prefix += "-" + namespace
	}
	return &BatchEncoder{options: opts, prefix: prefix, source: source,
		random: rand.New(source), encoder: encoder}, nil
}

func (g *BatchEncoder) Close() error { return g.encoder.Close() }

// Next appends the next compressed batch to dst and returns its metadata.
func (g *BatchEncoder) Next(dst []byte) (IndexEntry, []byte, error) {
	opts := g.options
	random, source := g.random, g.source
	marshal := proto.MarshalOptions{Deterministic: true}
	g.index++
	batch := &pb.EventBatch{
		BatchId:    fmt.Sprintf("%s-%06d", g.prefix, g.index),
		ProducerId: "payloadgen",
		Events:     make([]*pb.Event, 0, opts.EventsPerBatch),
	}
	for j := range opts.EventsPerBatch {
		asset := assetID(opts.Profile, random.IntN(opts.Assets)+1)
		var message proto.Message
		switch opts.Profile {
		case "solar":
			message = &pb.SolarReading{
				AssetId:       asset,
				PowerKw:       100 + random.Float64()*700,
				IrradianceWm2: random.Float64() * 1200,
				TemperatureC:  5 + random.Float64()*45,
				Status:        pb.Status_STATUS_OK,
			}
		case "synth-small":
			pad := make([]byte, 64)
			if _, err := source.Read(pad); err != nil {
				return IndexEntry{}, nil, err
			}
			message = &pb.SynthSmall{Value: random.Float64(), Pad: pad}
		case "synth-large":
			blob := make([]byte, 4096)
			if _, err := source.Read(blob); err != nil {
				return IndexEntry{}, nil, err
			}
			message = &pb.SynthLarge{Value: random.Float64(), Blob: blob}
		}
		payload, err := marshal.Marshal(message)
		if err != nil {
			return IndexEntry{}, nil, err
		}
		batch.Events = append(batch.Events, &pb.Event{
			EventId:         fmt.Sprintf("%s-%05d", batch.BatchId, j+1),
			RoutingKey:      asset,
			Profile:         opts.Profile,
			SchemaVersion:   1,
			EventTimeUnixMs: opts.Anchor.UnixMilli() - random.Int64N(time.Minute.Milliseconds()),
			Payload:         payload,
		})
	}
	raw, err := marshal.MarshalAppend(g.raw[:0], batch)
	if err != nil {
		return IndexEntry{}, nil, err
	}
	if len(raw) > maxRawBytes {
		return IndexEntry{}, nil, fmt.Errorf("batch %d exceeds the decoded body limit, reduce --events-per-batch", g.index)
	}
	g.raw = raw
	wire := g.encoder.EncodeAll(raw, dst)
	wireBytes := len(wire) - len(dst)
	if wireBytes > maxWireBytes {
		return IndexEntry{}, nil, fmt.Errorf("batch %d exceeds the wire body limit, reduce --events-per-batch", g.index)
	}
	name := fmt.Sprintf("batch-%06d.bin", g.index)
	return IndexEntry{name, batch.BatchId, len(batch.Events), wireBytes, len(raw)}, wire, nil
}

func WriteAssets(out io.Writer, profile string, count, tenants int) error {
	if profile != "solar" || count < 1 || tenants < 1 {
		return fmt.Errorf("assets requires profile solar and positive count and tenants")
	}
	buffer := bufio.NewWriter(out)
	for i := range count {
		if _, err := fmt.Fprintf(buffer,
			"INSERT INTO jammy.assets (asset_id, tenant_id, profile, capacity_kw) VALUES ('%s', 'tenant-%06d', 'solar', 1000.000) ON CONFLICT DO NOTHING;\n",
			assetID(profile, i+1), i%tenants+1); err != nil {
			return err
		}
	}
	return buffer.Flush()
}

func assetID(profile string, index int) string {
	return fmt.Sprintf("%s-%06d", profile, index)
}
