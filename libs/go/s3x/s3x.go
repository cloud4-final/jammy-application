package s3x

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cloud4-final/jammy-5th/libs/go/config"
)

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
