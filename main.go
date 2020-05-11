package main

import (
	"flag"
	"fmt"
	"os"
)

const cleanupHours = 12
const startedadDateFormat = "2006-01-02T15:04:05Z"

func main() {
	var endpoint, bucket string
	var skipTLSVerify, dryRun bool

	flag.StringVar(&endpoint, "endpoint", "", "Address of the S3 endpoint")
	flag.StringVar(&bucket, "bucket", "", "Bucket to use")
	flag.BoolVar(&dryRun, "dry-run", false, "Just print files to be deleted")
	flag.BoolVar(&skipTLSVerify, "skip-tls-verify", false, "Skip TLS certificate verification")

	flag.Parse()

	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")

	cleaner := NewS3Cleaner(endpoint, accessKey, secretKey, bucket, skipTLSVerify, dryRun)

	if err := cleaner.CleanMultipartUploads(); err != nil {
		fmt.Printf("failed to clean multipart uploads: %v\n", err)
		os.Exit(1)
	}
}
