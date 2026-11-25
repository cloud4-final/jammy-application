package pq

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"iter"
	"os"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/parquet-go/parquet-go"
	parquetzstd "github.com/parquet-go/parquet-go/compress/zstd"
)

const Stamp = "2006-01-02T15:04:05.000Z"

const (
	rowGroupRows  = 4096
	rowGroupBytes = 8 << 20
)

type Row struct {
	EventID       string  `parquet:"event_id"`
	RoutingKey    string  `parquet:"routing_key"`
	Profile       string  `parquet:"profile"`
	SchemaVersion int32   `parquet:"schema_version"`
	EventTime     int64   `parquet:"event_time,timestamp(millisecond:utc)"`
	ReceivedAt    int64   `parquet:"received_at,timestamp(millisecond:utc)"`
	IngestID      string  `parquet:"ingest_id"`
	PrimaryValue  float64 `parquet:"primary_value"`
	Payload       []byte  `parquet:"payload"`
}

type File struct {
	Rows, Size       int64
	SHA256           string
	MinTime, MaxTime time.Time
	SchemaVersion    int32
}

func Rows(data []Row) iter.Seq2[Row, error] {
	return func(yield func(Row, error) bool) {
		for _, row := range data {
			if !yield(row, nil) {
				return
			}
		}
	}
}

func Stage(dir string, rows []Row, level int) (string, File, error) {
	return StageFrom(dir, Rows(rows), level)
}

func StageFrom(dir string, rows iter.Seq2[Row, error], level int, options ...parquet.WriterOption) (path string, file File, err error) {
	temp, err := os.CreateTemp(dir, "jammy-*.parquet")
	if err != nil {
		return "", file, err
	}
	path = temp.Name()
	defer func() {
		err = errors.Join(err, temp.Close())
		if err != nil {
			_ = os.Remove(temp.Name())
		}
	}()
	hash := sha256.New()
	options = append(options,
		parquet.Compression(&parquetzstd.Codec{Level: zstd.EncoderLevelFromZstd(level), Concurrency: 1}),
		parquet.MaxRowsPerRowGroup(rowGroupRows),
		parquet.SkipPageBounds("payload"))
	w := parquet.NewGenericWriter[Row](io.MultiWriter(temp, hash), options...)
	buffer := make([]Row, 0, rowGroupRows)
	bufferBytes := 0
	flush := func() error {
		if len(buffer) == 0 {
			return nil
		}
		if _, err := w.Write(buffer); err != nil {
			return err
		}
		clear(buffer)
		buffer = buffer[:0]
		bufferBytes = 0
		return w.Flush()
	}
	for row, rowErr := range rows {
		if rowErr != nil {
			return "", file, rowErr
		}
		// Count the variable-width data and the int32 plus three 64-bit fields.
		// A row larger than the target is written in a group of its own.
		rowBytes := len(row.EventID) + len(row.RoutingKey) + len(row.Profile) + len(row.IngestID) + len(row.Payload) + 28
		if len(buffer) == cap(buffer) || bufferBytes+rowBytes > rowGroupBytes {
			if err = flush(); err != nil {
				return "", file, err
			}
		}
		ts := time.UnixMilli(row.EventTime).UTC()
		if file.Rows == 0 {
			file.MinTime, file.MaxTime, file.SchemaVersion = ts, ts, row.SchemaVersion
		}
		if row.SchemaVersion != file.SchemaVersion {
			return "", file, errors.New("mixed Parquet schema versions")
		}
		if ts.Before(file.MinTime) {
			file.MinTime = ts
		}
		if ts.After(file.MaxTime) {
			file.MaxTime = ts
		}
		file.Rows++
		buffer = append(buffer, row)
		bufferBytes += rowBytes
		if bufferBytes >= rowGroupBytes {
			if err = flush(); err != nil {
				return "", file, err
			}
		}
	}
	if err = flush(); err != nil {
		return "", file, err
	}
	if err = w.Close(); err != nil {
		return "", file, err
	}
	file.Size, err = temp.Seek(0, io.SeekCurrent)
	file.SHA256 = hex.EncodeToString(hash.Sum(nil))
	return path, file, err
}
