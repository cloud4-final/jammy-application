package s3x

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestClientSignsPathStyleCustomEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/jammy/object" || !strings.Contains(r.Header.Get("Authorization"), "Credential=test/") {
			t.Errorf("unexpected request path %q or missing request signature", r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	t.Setenv("S3_ENDPOINT", server.URL)
	t.Setenv("S3_FORCE_PATH_STYLE", "true")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	client, err := New(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	options := client.Options()
	if options.RequestChecksumCalculation != aws.RequestChecksumCalculationWhenRequired ||
		options.ResponseChecksumValidation != aws.ResponseChecksumValidationWhenRequired {
		t.Fatal("S3 client must calculate and validate checksums only when required")
	}
	bucket, key := "jammy", "object"
	if _, err := client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: &bucket, Key: &key}); err != nil {
		t.Fatal(err)
	}
}
