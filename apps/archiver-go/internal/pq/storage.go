package pq

import (
	"bytes"
	"context"
	"crypto/sha256"
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
	"github.com/cloud4-final/jammy-5th/apps/archiver-go/internal/ratelimit"
	"github.com/parquet-go/parquet-go"
)

type Objects interface {
	// Put creates an object only when its key does not exist.
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

func uploadOptions(o *s3.Options) {
	o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
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
		return s.multipart(ctx, key, src, size, media, max(initialPart, 1+(size-1)/10000))
	}
	_, err := s.Client.PutObject(ctx, &s3.PutObjectInput{Bucket: &s.Bucket, Key: &key,
		Body: ratelimit.New(ctx, body, s.Limit), ContentLength: &size, ContentType: &media, IfNoneMatch: aws.String("*")}, uploadOptions)
	return err
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
			PartNumber: &number, Body: body, ContentLength: &length}, uploadOptions)
		if err != nil {
			return err
		}
		completed = append(completed, types.CompletedPart{PartNumber: &number, ETag: part.ETag})
	}
	_, err = s.Client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: &s.Bucket, Key: &key, UploadId: start.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completed}, IfNoneMatch: aws.String("*")})
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
	Version       int     `json:"manifest_version"`
	Rows          int64   `json:"rows"`
	SHA256        string  `json:"sha256"`
	MinTime       string  `json:"min_event_time"`
	MaxTime       string  `json:"max_event_time"`
	Source        string  `json:"source"`
	Stream        *string `json:"stream"`
	FirstSeq      *uint64 `json:"first_seq"`
	LastSeq       *uint64 `json:"last_seq"`
	Partition     *string `json:"partition_name"`
	SchemaVersion int32   `json:"schema_version"`
}

func ManifestKey(key string) string { return strings.TrimSuffix(key, ".parquet") + ".manifest.json" }

type Archive struct {
	Objects  Objects
	Dir      string
	Level    int
	Uploaded func(context.Context, int64, time.Duration)
}

func (a Archive) Write(ctx context.Context, key string, rows iter.Seq2[Row, error], manifest Manifest, verifyUpload bool) (Ref, Manifest, error) {
	path, file, err := StageFrom(a.Dir, rows, a.Level)
	if err != nil {
		return Ref{}, manifest, err
	}
	defer func() { _ = os.Remove(path) }()
	ref := Ref{key, file.Rows, file.SHA256}
	exists, err := a.Objects.Exists(ctx, key)
	if err != nil {
		return Ref{}, manifest, fmt.Errorf("check archive %s: %w", key, err)
	}
	if !exists {
		err := a.upload(ctx, path, key, file.Size)
		if isPreconditionFailed(err) {
			// Another writer created the key after the existence check.
			exists = true
		} else if err != nil {
			return Ref{}, manifest, fmt.Errorf("upload archive %s: %w", key, err)
		}
	}
	if exists || verifyUpload {
		if err := a.Verify(ctx, ref); err != nil {
			return Ref{}, manifest, fmt.Errorf("verify archive %s: %w", key, err)
		}
	}
	manifest.Version = 1
	manifest.Rows, manifest.SHA256, manifest.SchemaVersion = file.Rows, file.SHA256, file.SchemaVersion
	manifest.MinTime, manifest.MaxTime = file.MinTime.Format(Stamp), file.MaxTime.Format(Stamp)
	_, err = PutJSON(ctx, a.Objects, ManifestKey(key), &manifest, nil)
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

func (a Archive) Verify(ctx context.Context, ref Ref) error {
	body, err := a.Objects.Get(ctx, ref.Key)
	if err != nil {
		return err
	}
	defer func() { _ = body.Close() }()
	file, err := os.CreateTemp(a.Dir, "jammy-verify-*.parquet")
	if err != nil {
		return err
	}
	defer func() { _ = file.Close(); _ = os.Remove(file.Name()) }()
	hash := sha256.New()
	size, err := io.Copy(io.MultiWriter(file, hash), body)
	if err != nil {
		return err
	}
	if hex.EncodeToString(hash.Sum(nil)) != ref.SHA256 {
		return errors.New("archived Parquet checksum differs")
	}
	// Only the footer metadata is needed for the row count.
	decoded, err := parquet.OpenFile(file, size, parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true))
	if err != nil {
		return err
	}
	if decoded.NumRows() != ref.Rows {
		return fmt.Errorf("archived Parquet row count differs: got %d, expected %d", decoded.NumRows(), ref.Rows)
	}
	return nil
}

func isPreconditionFailed(err error) bool {
	var response *awshttp.ResponseError
	return errors.As(err, &response) && response.HTTPStatusCode() == 412
}

// PutJSON reuses an existing object with identical bytes. For documents with a
// generated timestamp, matches can validate the other fields and reuse the
// stored document, including its original timestamp.
func PutJSON[T any](ctx context.Context, store Objects, key string, value *T, matches func(T) bool) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	exists, err := store.Exists(ctx, key)
	if err != nil {
		return "", fmt.Errorf("check archive %s: %w", key, err)
	}
	if !exists {
		err := store.Put(ctx, key, bytes.NewReader(data), int64(len(data)), "application/json")
		if err == nil {
			hash := sha256.Sum256(data)
			return hex.EncodeToString(hash[:]), nil
		}
		if !isPreconditionFailed(err) {
			return "", fmt.Errorf("upload archive %s: %w", key, err)
		}
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
		if matches == nil || json.Unmarshal(stored, &existing) != nil || !matches(existing) {
			return "", fmt.Errorf("existing archive manifest differs for %s", key)
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
