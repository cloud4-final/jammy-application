package codec

import (
	"bytes"
	"errors"
	"io"
	"os"
	"runtime"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestZstdBombRejected413(t *testing.T) {
	// 800 maximum-size RLE blocks produce 100 MiB without a frame content
	// size. This exercises the streaming limit rather than the header check.
	wire := []byte{0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x38}
	for range 800 {
		wire = append(wire, 0x02, 0x00, 0x10, 0x00)
	}
	wire[len(wire)-4] |= 1 // Last-block bit.
	decoder, err := zstd.NewReader(bytes.NewReader(wire), zstd.WithDecoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := io.Copy(io.Discard, decoder)
	decoder.Close()
	if err != nil || decoded != 100<<20 {
		t.Fatalf("fixture decoded to %d bytes: %v", decoded, err)
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, _, err = DecodeBatch(wire, "application/x-protobuf", "zstd", 16<<20, 64<<20)
	runtime.ReadMemStats(&after)
	t.Logf("wire=%d bytes, decoded=%d bytes, allocated=%d bytes", len(wire), decoded, after.TotalAlloc-before.TotalAlloc)
	assertHTTPError(t, err, 413)
	// TotalAlloc bounds even a transient heap peak during decode.
	if increase := after.TotalAlloc - before.TotalAlloc; increase >= 80<<20 {
		t.Fatalf("decode allocated %d bytes, want <80 MiB", increase)
	}
}

func TestZstdRequiresSingleCompleteFrame(t *testing.T) {
	payload, err := os.ReadFile("../../../contracts/testdata/gate_cases/valid-solar.bin")
	if err != nil {
		t.Fatal(err)
	}
	encoder, err := zstd.NewWriter(io.Discard, zstd.WithEncoderCRC(true))
	if err != nil {
		t.Fatal(err)
	}
	frame := encoder.EncodeAll(payload, nil)
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	if _, raw, err := DecodeBatch(frame, "application/x-protobuf", " ZSTD ", int64(len(frame)), int64(len(payload))); err != nil || !bytes.Equal(raw, payload) {
		t.Fatalf("valid frame decode: %v", err)
	}
	modified := func(tail ...byte) []byte { return append(bytes.Clone(frame), tail...) }
	badChecksum := modified()
	badChecksum[len(badChecksum)-1] ^= 1
	for name, body := range map[string][]byte{
		"truncated":    frame[:len(frame)-1],
		"checksum":     badChecksum,
		"trailing":     modified(0x00),
		"concatenated": modified(frame...),
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := DecodeBatch(body, "application/x-protobuf", "zstd", 16<<20, 64<<20)
			assertHTTPError(t, err, 400)
		})
	}
}

func assertHTTPError(t *testing.T, err error, status int) {
	t.Helper()
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.Status != status {
		t.Fatalf("decode error = %v, want HTTP %d", err, status)
	}
}

func TestJSONRejectsDuplicateKeysAndMalformedBase64(t *testing.T) {
	for name, body := range map[string]string{
		"batch_duplicate_key": `{"batchId":"one","batchId":"two","events":[{}]}`,
		"event_duplicate_key": `{"batchId":"one","events":[{"eventId":"a","eventId":"b"}]}`,
		"attrs_duplicate_key": `{"batchId":"one","events":[{"attrs":{"a":"one","a":"two"}}]}`,
		"base64_padding":      `{"batchId":"one","events":[{"payload":"AQ="}]}`,
		"base64_newlines":     `{"batchId":"one","events":[{"payload":"AQ==\r\n\r\n"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := DecodeBatch([]byte(body), "application/json", "", 1024, 1024)
			assertHTTPError(t, err, 400)
		})
	}
}

func TestJSONPreservesExactInt64AndAttributeNames(t *testing.T) {
	for _, timestamp := range []string{`"9223372036854775807e0"`, "9223372036854775807.0"} {
		t.Run(timestamp, func(t *testing.T) {
			body := `{"batchId":"one","events":[{"eventTimeUnixMs":` + timestamp + `,"attrs":{"eventId":"a","event_id":"b"}}]}`
			batch, _, err := DecodeBatch([]byte(body), " APPLICATION/JSON ; charset=utf-8", "", 1024, 1024)
			if err != nil {
				t.Fatal(err)
			}
			event := batch.GetEvents()[0]
			if event.GetEventTimeUnixMs() != 9223372036854775807 ||
				event.GetAttrs()["eventId"] != "a" || event.GetAttrs()["event_id"] != "b" {
				t.Fatalf("unexpected event %v", event)
			}
		})
	}
}
