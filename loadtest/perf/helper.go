// The helper reads physical Parquet footer row counts.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"
)

type objectReader struct {
	ctx               context.Context
	client            *s3.Client
	bucket, key, etag string
}

func (r objectReader) ReadAt(p []byte, offset int64) (int, error) {
	result, err := r.client.GetObject(r.ctx, &s3.GetObjectInput{
		Bucket: &r.bucket, Key: &r.key, IfMatch: &r.etag,
		Range: aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+int64(len(p))-1)),
	})
	if err != nil {
		return 0, err
	}
	defer func() { _ = result.Body.Close() }()
	return io.ReadFull(result.Body, p)
}

type objectRows struct {
	ETag string `json:"etag"`
	Rows int64  `json:"rows"`
}

func parquetRows(ctx context.Context, cachePath, endpoint string) error {
	client := s3.NewFromConfig(aws.Config{
		Region:      "ap-northeast-2",
		Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider("jammy", "jammy", "")),
	}, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(endpoint)
	})
	const bucket = "jammy-local"
	cache := map[string]objectRows{}
	if data, err := os.ReadFile(cachePath); err == nil {
		if err = json.Unmarshal(data, &cache); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	current := map[string]objectRows{}
	for _, prefix := range []string{"raw/", "raw-late/"} {
		pages := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
			Bucket: aws.String(bucket), Prefix: aws.String(prefix),
		})
		for pages.HasMorePages() {
			page, err := pages.NextPage(ctx)
			if err != nil {
				return err
			}
			for _, object := range page.Contents {
				key, etag := aws.ToString(object.Key), aws.ToString(object.ETag)
				if !strings.HasSuffix(key, ".parquet") {
					continue
				}
				item, cached := cache[key]
				if !cached || item.ETag != etag {
					r := objectReader{ctx, client, bucket, key, etag}
					file, err := parquet.OpenFile(r, aws.ToInt64(object.Size),
						parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true))
					if err != nil {
						return fmt.Errorf("read %s: %w", key, err)
					}
					item = objectRows{etag, file.NumRows()}
				}
				current[key] = item
			}
		}
	}
	data, err := json.MarshalIndent(current, "", "  ")
	if err != nil {
		return err
	}
	if err = os.WriteFile(cachePath, append(data, '\n'), 0644); err != nil {
		return err
	}
	var rows int64
	for _, item := range current {
		rows += item.Rows
	}
	fmt.Println(rows)
	return nil
}

func run(args []string) error {
	if len(args) == 0 || args[0] != "parquet-rows" {
		return fmt.Errorf("usage: helper parquet-rows [--s3-endpoint URL] CACHE")
	}
	endpoint := os.Getenv("S3_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://127.0.0.1:8333"
	}
	flags := flag.NewFlagSet("parquet-rows", flag.ContinueOnError)
	flags.StringVar(&endpoint, "s3-endpoint", endpoint, "S3 endpoint URL")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return fmt.Errorf("usage: helper parquet-rows [--s3-endpoint URL] CACHE")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	return parquetRows(ctx, flags.Arg(0), endpoint)
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}
