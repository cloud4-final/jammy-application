package ratelimit

import (
	"bytes"
	"io"
	"testing"
	"time"
)

func TestUploadRateLimitRespected(t *testing.T) {
	data := make([]byte, 5<<20)
	start := time.Now()
	n, err := io.Copy(io.Discard, New(t.Context(), bytes.NewReader(data), 1<<20))
	if err != nil || n != int64(len(data)) || time.Since(start) < 4500*time.Millisecond {
		t.Fatalf("read %d bytes in %s: %v", n, time.Since(start), err)
	}
}
