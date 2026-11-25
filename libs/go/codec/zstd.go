package codec

import (
	"bytes"
	"errors"
	"io"
	"math"
	"net/http"

	"github.com/klauspost/compress/zstd"
)

func decodeZstd(body []byte, maxSize int64) ([]byte, error) {
	invalid := &HTTPError{http.StatusBadRequest, "invalid zstd frame"}
	oversized := &HTTPError{http.StatusRequestEntityTooLarge, "decoded body exceeds size limit"}
	var header zstd.Header
	if err := header.Decode(body); err != nil || !singleFrame(body, header) {
		return nil, invalid
	}
	if maxSize < 0 {
		return nil, oversized
	}
	// Stop streaming at the first byte beyond the limit. A known decoded
	// size can reject very large single-segment frames before allocating.
	if header.HasFCS && header.FrameContentSize > uint64(maxSize) {
		return nil, oversized
	}
	decoder, err := zstd.NewReader(bytes.NewReader(body),
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderLowmem(true),
		zstd.WithDecoderMaxMemory(uint64(max(maxSize, zstd.MinWindowSize))),
		zstd.WithDecodeBuffersBelow(0),
	)
	if err != nil {
		return nil, invalid
	}
	defer decoder.Close()

	// Read in chunks so an oversized body does not grow and copy a large
	// buffer. Combine chunks only once the complete frame has been verified.
	limit := min(maxSize, math.MaxInt64-1) + 1
	reader := io.LimitReader(decoder, limit)
	var chunks [][]byte
	var total int64
	for {
		chunk := make([]byte, max(1, min(32<<10, limit-total)))
		var offset int
		for offset < len(chunk) {
			n, err := reader.Read(chunk[offset:])
			offset += n
			if total+int64(offset) > maxSize || errors.Is(err, zstd.ErrDecoderSizeExceeded) || errors.Is(err, zstd.ErrWindowSizeExceeded) {
				return nil, oversized
			}
			if err == io.EOF {
				if offset > 0 {
					chunks = append(chunks, chunk[:offset])
				}
				return joinChunks(chunks), nil
			}
			if err != nil {
				return nil, invalid
			}
		}
		chunks = append(chunks, chunk)
		total += int64(offset)
	}
}

func joinChunks(chunks [][]byte) []byte {
	if len(chunks) == 1 {
		return chunks[0]
	}
	return bytes.Join(chunks, nil)
}

// singleFrame walks compressed block boundaries. The decoder then verifies
// block contents, declared decoded size and the optional frame checksum.
func singleFrame(body []byte, header zstd.Header) bool {
	if header.Skippable {
		return false
	}
	remaining := body[header.HeaderSize:]
	for len(remaining) >= 3 {
		block := uint32(remaining[0]) | uint32(remaining[1])<<8 | uint32(remaining[2])<<16
		size := int(block >> 3)
		switch (block >> 1) & 3 {
		case 1: // RLE has a one-byte encoded payload.
			size = 1
		case 3: // reserved block type
			return false
		}
		remaining = remaining[3:]
		if size > len(remaining) {
			return false
		}
		remaining = remaining[size:]
		if block&1 != 0 {
			if header.HasCheckSum {
				return len(remaining) == 4
			}
			return len(remaining) == 0
		}
	}
	return false
}
