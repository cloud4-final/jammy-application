package ratelimit

import (
	"context"
	"io"
	"time"
)

type Reader struct {
	io.ReadSeeker
	ctx   context.Context
	limit int64
	start time.Time
	read  int64
}

func New(ctx context.Context, source io.ReadSeeker, limit int64) *Reader {
	return &Reader{ReadSeeker: source, ctx: ctx, limit: limit}
}

func (r *Reader) Read(buf []byte) (int, error) {
	if r.limit == 0 {
		return r.ReadSeeker.Read(buf)
	}
	if r.start.IsZero() {
		r.start = time.Now()
	}
	n, err := r.ReadSeeker.Read(buf)
	r.read += int64(n)
	wait := time.Duration(float64(r.read)/float64(r.limit)*float64(time.Second)) - time.Since(r.start)
	if wait <= 0 {
		return n, err
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return n, err
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	}
}
