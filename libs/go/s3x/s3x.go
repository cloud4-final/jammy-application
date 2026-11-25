package s3x

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cloud4-final/jammy-application/libs/go/config"
)

// NormalizePrefix trims leading and trailing slashes and appends a single
// trailing slash when non-empty.
func NormalizePrefix(raw string) string {
	trimmed := strings.Trim(raw, "/")
	if trimmed == "" {
		return ""
	}
	return trimmed + "/"
}

// Prefix reads S3_PREFIX (with S3_PREFIX_FILE precedence) and normalizes it.
func Prefix() (string, error) {
	raw, err := config.Lookup("S3_PREFIX", "")
	if err != nil {
		return "", err
	}
	return NormalizePrefix(raw), nil
}

// New creates an S3 client with the SDK's standard credential provider chain.
func New(ctx context.Context) (*s3.Client, error) {
	region, err := config.Lookup("S3_REGION", "ap-northeast-2")
	if err != nil {
		return nil, err
	}
	endpoint, err := config.Lookup("S3_ENDPOINT", "")
	if err != nil {
		return nil, err
	}
	if endpoint != "" {
		parsed, err := url.Parse(endpoint)
		if err != nil || parsed.Hostname() == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return nil, errors.New("invalid S3_ENDPOINT")
		}
	}
	pathStyle, err := config.Lookup("S3_FORCE_PATH_STYLE", "false")
	if err != nil {
		return nil, err
	}
	forcePathStyle, err := config.ParseBool(pathStyle)
	if err != nil {
		return nil, fmt.Errorf("invalid S3_FORCE_PATH_STYLE: %w", err)
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, err
	}
	return s3.NewFromConfig(cfg, func(options *s3.Options) {
		options.UsePathStyle = forcePathStyle
		options.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		options.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		if endpoint != "" {
			options.BaseEndpoint = aws.String(endpoint)
		}
	}), nil
}
