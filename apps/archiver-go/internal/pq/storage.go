package pq

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/cloud4-final/jammy-application/apps/archiver-go/internal/ratelimit"
	"github.com/parquet-go/parquet-go"
)

type Objects interface {
	// Put writes or replaces an object.
	Put(context.Context, string, io.ReadSeeker, int64, string) error
	Get(context.Context, string) (io.ReadCloser, error)
	Exists(context.Context, string) (bool, error)
	List(context.Context, string) ([]string, error)
}

const parquetMediaType = "application/vnd.apache.parquet"

type S3 struct {
	Client *s3.Client
	Bucket string
	Limit  int64
	// UploadSlot shares the rate limit across stream workers when configured.
	UploadSlot chan struct{}
}

// Avoid reading the rate-limited body twice, once to sign and once to transmit.
func signWithoutPayloadHash(o *s3.Options) {
	o.APIOptions = append(o.APIOptions, v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware)
}

func (s S3) Put(ctx context.Context, key string, body io.ReadSeeker, size int64, media string) error {
	if s.UploadSlot != nil {
		select {
		case s.UploadSlot <- struct{}{}:
			defer func() { <-s.UploadSlot }()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	const initialPart = int64(64 << 20)
	if src, ok := body.(io.ReaderAt); ok && size > initialPart {
		return s.multipart(ctx, key, src, size, media, multipartPartSize(size))
	}
	_, err := s.Client.PutObject(ctx, &s3.PutObjectInput{Bucket: &s.Bucket, Key: &key,
		Body: ratelimit.New(ctx, body, s.Limit), ContentLength: &size, ContentType: &media}, signWithoutPayloadHash)
	return err
}

func multipartPartSize(size int64) int64 {
	const initialPart = int64(64 << 20)
	if size <= 0 {
		return initialPart
	}
	return max(initialPart, 1+(size-1)/10000)
}

func (s S3) multipart(ctx context.Context, key string, src io.ReaderAt, size int64, media string, partSize int64) (err error) {
	start, err := s.Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: &s.Bucket, Key: &key, ContentType: &media})
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			stop, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			_, _ = s.Client.AbortMultipartUpload(stop, &s3.AbortMultipartUploadInput{Bucket: &s.Bucket, Key: &key, UploadId: start.UploadId})
		}
	}()
	var completed []types.CompletedPart
	for offset := int64(0); offset < size; offset += partSize {
		length := min(partSize, size-offset)
		number := int32(len(completed) + 1)
		body := ratelimit.New(ctx, io.NewSectionReader(src, offset, length), s.Limit)
		part, err := s.Client.UploadPart(ctx, &s3.UploadPartInput{Bucket: &s.Bucket, Key: &key, UploadId: start.UploadId,
			PartNumber: &number, Body: body, ContentLength: &length}, signWithoutPayloadHash)
		if err != nil {
			return err
		}
		completed = append(completed, types.CompletedPart{PartNumber: &number, ETag: part.ETag})
	}
	_, err = s.Client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: &s.Bucket, Key: &key, UploadId: start.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completed}})
	return err
}

func (s S3) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	output, err := s.Client.GetObject(ctx, &s3.GetObjectInput{Bucket: &s.Bucket, Key: &key})
	if err != nil {
		return nil, err
	}
	return output.Body, nil
}

func (s S3) Exists(ctx context.Context, key string) (bool, error) {
	_, err := s.Client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &s.Bucket, Key: &key})
	if err != nil {
		var response *awshttp.ResponseError
		if errors.As(err, &response) && response.HTTPStatusCode() == 404 {
			return false, nil
		}
	}
	return err == nil, err
}

func (s S3) List(ctx context.Context, prefix string) ([]string, error) {
	pages := s3.NewListObjectsV2Paginator(s.Client, &s3.ListObjectsV2Input{Bucket: &s.Bucket, Prefix: &prefix})
	var keys []string
	for pages.HasMorePages() {
		response, err := pages.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, key := range response.Contents {
			keys = append(keys, aws.ToString(key.Key))
		}
	}
	return keys, nil
}

type Ref struct {
	Key    string `json:"key"`
	Rows   int64  `json:"rows"`
	SHA256 string `json:"sha256"`
}

type Manifest struct {
	Version        int         `json:"manifest_version"`
	Rows           int64       `json:"rows"`
	SHA256         string      `json:"sha256"`
	MinTime        string      `json:"min_event_time"`
	MaxTime        string      `json:"max_event_time"`
	Source         string      `json:"source"`
	Stream         *string     `json:"stream"`
	FirstSeq       *uint64     `json:"first_seq"`
	LastSeq        *uint64     `json:"last_seq"`
	Partition      *string     `json:"partition_name"`
	SchemaVersion  int32       `json:"schema_version"`
	SequenceRanges [][2]uint64 `json:"sequence_ranges,omitempty"`
}

const jetstreamMetadata = "jammy.jetstream"

type coverage struct {
	Stream *string     `json:"stream"`
	Ranges [][2]uint64 `json:"sequence_ranges"`
}

func ManifestKey(key string) string { return strings.TrimSuffix(key, ".parquet") + ".manifest.json" }

var ErrManifestConflict = errors.New("existing archive manifest differs")

type Archive struct {
	Objects  Objects
	Dir      string
	Level    int
	Uploaded func(context.Context, int64, time.Duration)
	Written  func(context.Context)
}

func (a Archive) Write(ctx context.Context, key string, rows iter.Seq2[Row, error], manifest Manifest) (Ref, Manifest, error) {
	var options []parquet.WriterOption
	if len(manifest.SequenceRanges) != 0 {
		data, err := json.Marshal(coverage{manifest.Stream, manifest.SequenceRanges})
		if err != nil {
			return Ref{}, manifest, err
		}
		options = append(options, parquet.KeyValueMetadata(jetstreamMetadata, string(data)))
	}
	path, file, err := StageFrom(a.Dir, rows, a.Level, options...)
	if err != nil {
		return Ref{}, manifest, err
	}
	defer func() { _ = os.Remove(path) }()
	return a.commitStaged(ctx, path, file, key, manifest)
}

func (a Archive) commitStaged(ctx context.Context, path string, file File, key string, manifest Manifest) (Ref, Manifest, error) {
	if err := ValidateFile(path, file.Size, file.Rows); err != nil {
		return Ref{}, manifest, fmt.Errorf("validate staged archive %s: %w", key, err)
	}
	ref := Ref{key, file.Rows, file.SHA256}
	if err := a.upload(ctx, path, key, file.Size); err != nil {
		return Ref{}, manifest, fmt.Errorf("upload archive %s: %w", key, err)
	}
	if err := a.Verify(ctx, ref); err != nil {
		return Ref{}, manifest, fmt.Errorf("verify archive %s: %w", key, err)
	}
	manifest.Version = 1
	manifest.Rows, manifest.SHA256, manifest.SchemaVersion = file.Rows, file.SHA256, file.SchemaVersion
	manifest.MinTime, manifest.MaxTime = file.MinTime.Format(Stamp), file.MaxTime.Format(Stamp)
	_, err := PutJSON(ctx, a.Objects, ManifestKey(key), &manifest, nil)
	if err == nil && a.Written != nil {
		a.Written(ctx)
	}
	return ref, manifest, err
}

func (a Archive) upload(ctx context.Context, path, key string, size int64) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	start := time.Now()
	if err := a.Objects.Put(ctx, key, f, size, parquetMediaType); err != nil {
		return err
	}
	if a.Uploaded != nil {
		a.Uploaded(ctx, size, time.Since(start))
	}
	return nil
}

var expectedColumns = []struct {
	name, typ string
}{
	{"event_id", "STRING"},
	{"routing_key", "STRING"},
	{"profile", "STRING"},
	{"schema_version", "INT(32,true)"},
	{"event_time", "TIMESTAMP(isAdjustedToUTC=true,unit=MILLIS)"},
	{"received_at", "TIMESTAMP(isAdjustedToUTC=true,unit=MILLIS)"},
	{"ingest_id", "STRING"},
	{"primary_value", "DOUBLE"},
	{"payload", "BYTE_ARRAY"},
}

func validateParquet(decoded *parquet.File, expectedRows int64) error {
	if decoded.NumRows() != expectedRows {
		return fmt.Errorf("parquet row count differs: got %d, expected %d", decoded.NumRows(), expectedRows)
	}
	fields := decoded.Schema().Fields()
	if len(fields) != len(expectedColumns) {
		return fmt.Errorf("parquet schema column count differs: got %d, expected %d", len(fields), len(expectedColumns))
	}
	for i, want := range expectedColumns {
		field := fields[i]
		if !field.Required() || field.Name() != want.name || field.Type().String() != want.typ {
			return fmt.Errorf("parquet schema mismatch at column %d (%s): got %s required=%v", i, field.Name(), field.Type().String(), field.Required())
		}
	}
	for _, group := range decoded.Metadata().RowGroups {
		for _, col := range group.Columns {
			if col.MetaData.Codec.String() != "ZSTD" {
				return fmt.Errorf("parquet column codec differs: got %s, expected ZSTD", col.MetaData.Codec.String())
			}
		}
	}
	return nil
}

// ValidateFile opens a staged Parquet file and verifies its footer, contract schema, codec, and row count.
func ValidateFile(path string, size, expectedRows int64) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	decoded, err := parquet.OpenFile(f, size, parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true))
	if err != nil {
		return err
	}
	return validateParquet(decoded, expectedRows)
}

func (a Archive) Verify(ctx context.Context, ref Ref) error {
	decoded, _, err := a.download(ctx, ref.Key, ref.SHA256)
	if err != nil {
		return err
	}
	return validateParquet(decoded, ref.Rows)
}

func (a Archive) download(ctx context.Context, key, expectedHash string) (*parquet.File, string, error) {
	body, err := a.Objects.Get(ctx, key)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = body.Close() }()
	file, err := os.CreateTemp(a.Dir, "jammy-verify-*.parquet")
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = file.Close(); _ = os.Remove(file.Name()) }()
	hash := sha256.New()
	size, err := io.Copy(io.MultiWriter(file, hash), body)
	if err != nil {
		return nil, "", err
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	if expectedHash != "" && digest != expectedHash {
		return nil, "", errors.New("archived Parquet checksum differs")
	}
	// Callers use only the footer, which is loaded before the file closes.
	decoded, err := parquet.OpenFile(file, size, parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true))
	return decoded, digest, err
}

// Inspect reconstructs a missing manifest from the persisted Parquet footer.
func (a Archive) Inspect(ctx context.Context, key string) (Manifest, error) {
	file, digest, err := a.download(ctx, key, "")
	if err != nil {
		return Manifest{}, err
	}
	value, ok := file.Lookup(jetstreamMetadata)
	if !ok {
		return Manifest{}, nil
	}
	var sequences coverage
	if err := json.Unmarshal([]byte(value), &sequences); err != nil {
		return Manifest{}, err
	}
	manifest := Manifest{Version: 1, Source: "jetstream", Stream: sequences.Stream, SequenceRanges: sequences.Ranges,
		Rows: file.NumRows(), SHA256: digest}
	var oldest, newest time.Time
	for _, group := range file.Metadata().RowGroups {
		if len(group.Columns) <= 4 {
			return Manifest{}, errors.New("invalid archive footer schema")
		}
		// schema_version and event_time have fixed physical column indexes.
		version := group.Columns[3].MetaData.Statistics.MinValue
		stats := group.Columns[4].MetaData.Statistics
		if len(version) != 4 || len(stats.MinValue) != 8 || len(stats.MaxValue) != 8 {
			return Manifest{}, errors.New("archive footer is missing column statistics")
		}
		manifest.SchemaVersion = int32(binary.LittleEndian.Uint32(version))
		first := time.UnixMilli(int64(binary.LittleEndian.Uint64(stats.MinValue))).UTC()
		last := time.UnixMilli(int64(binary.LittleEndian.Uint64(stats.MaxValue))).UTC()
		if oldest.IsZero() || first.Before(oldest) {
			oldest = first
		}
		if newest.IsZero() || last.After(newest) {
			newest = last
		}
	}
	manifest.MinTime, manifest.MaxTime = oldest.Format(Stamp), newest.Format(Stamp)
	return manifest, nil
}

// PutJSON writes or replaces the document. When matches is provided, reuse
// the existing document after checking its contents, preserving its timestamp.
func PutJSON[T any](ctx context.Context, store Objects, key string, value *T, matches func(T) bool) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	var exists bool
	if matches != nil {
		exists, err = store.Exists(ctx, key)
		if err != nil {
			return "", fmt.Errorf("check archive %s: %w", key, err)
		}
	}
	if !exists {
		if err := store.Put(ctx, key, bytes.NewReader(data), int64(len(data)), "application/json"); err != nil {
			return "", fmt.Errorf("upload archive %s: %w", key, err)
		}
		hash := sha256.Sum256(data)
		return hex.EncodeToString(hash[:]), nil
	}
	body, err := store.Get(ctx, key)
	if err != nil {
		return "", fmt.Errorf("read existing archive %s: %w", key, err)
	}
	defer func() { _ = body.Close() }()
	stored, err := io.ReadAll(body)
	if err != nil {
		return "", fmt.Errorf("read existing archive %s: %w", key, err)
	}
	if !bytes.Equal(data, stored) {
		var existing T
		if json.Unmarshal(stored, &existing) != nil || !matches(existing) {
			return "", fmt.Errorf("%w for %s", ErrManifestConflict, key)
		}
		*value = existing
	}
	hash := sha256.Sum256(stored)
	return hex.EncodeToString(hash[:]), nil
}

func ReadJSON(ctx context.Context, store Objects, key string, value any) error {
	body, err := store.Get(ctx, key)
	if err != nil {
		return err
	}
	defer func() { _ = body.Close() }()
	return json.NewDecoder(body).Decode(value)
}
