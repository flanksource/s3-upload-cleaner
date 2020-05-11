package main

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
)

type S3Cleaner struct {
	s3     *s3.S3
	bucket string
	dryRun bool
}

func NewS3Cleaner(endpoint, accessKey, secretKey, bucket string, skipTLSVerify, dryRun bool) *S3Cleaner {
	s3Client := getS3Client(endpoint, accessKey, secretKey, skipTLSVerify)
	cleaner := &S3Cleaner{
		s3:     s3Client,
		bucket: bucket,
		dryRun: dryRun,
	}
	return cleaner
}

func (c *S3Cleaner) CleanMultipartUploads() error {
	totalRemoved := 0

	fmt.Printf("Endpoint: %s\n", *c.s3.Config.Endpoint)
	fmt.Printf("Bucket: %s\n\n", c.bucket)

	var marker *string = nil
	var maxKeys int64 = 100

	for {
		objs, err := c.s3.ListObjects(&s3.ListObjectsInput{
			Bucket:    aws.String(c.bucket),
			Prefix:    aws.String("docker/registry/v2/repositories/"),
			Delimiter: aws.String("/"),
			Marker:    marker,
			MaxKeys:   aws.Int64(maxKeys),
		})

		if err != nil {
			return errors.Wrap(err, "failed to list objects")
		}

		for i, cp := range objs.CommonPrefixes {
			fmt.Printf("Prefix %d: %s\n", i, aws.StringValue(cp.Prefix))

			removed, err := c.cleanMPUs(aws.StringValue(cp.Prefix))
			if err != nil {
				return errors.Wrapf(err, "failed to remove multipart uploads for prefix %s", aws.StringValue(cp.Prefix))
			}

			totalRemoved += removed
			fmt.Printf("  Total MPUs removed: %d\n", totalRemoved)
		}

		fmt.Println()
		fmt.Println("Removing upload folders:")
		if err := c.cleanUploadFolders(aws.StringValue(objs.Prefix)); err != nil {
			return errors.Wrapf(err, "failed to clean upload folders for prefix %s", aws.StringValue(objs.Prefix))
		}

		if aws.BoolValue(objs.IsTruncated) && len(objs.Contents) > 0 {
			marker = objs.Contents[len(objs.Contents)-1].Key
		} else {
			break
		}
	}

	return nil
}

func (c *S3Cleaner) cleanMPUs(prefix string) (int, error) {
	totalRemoved := 0
	var keyMarker *string = nil
	var uploadIDMarker *string = nil

	for {
		resp, err := c.s3.ListMultipartUploads(&s3.ListMultipartUploadsInput{
			Bucket:         aws.String(c.bucket),
			Prefix:         aws.String(prefix),
			MaxUploads:     aws.Int64(1000),
			KeyMarker:      keyMarker,
			UploadIdMarker: uploadIDMarker,
		})

		if err != nil {
			return 0, errors.Wrap(err, "failed to list multipart uploads")
		}

		fmt.Printf(" # of MPUs found for prefix: %d\n", len(resp.Uploads))

		for i, multi := range resp.Uploads {
			fmt.Printf("  Upload %d: %s\n", i, aws.StringValue(multi.Key))

			hoursSince := int(time.Since(aws.TimeValue(multi.Initiated)).Hours())

			fmt.Printf("  Started %d hours ago\n", hoursSince)

			if hoursSince > cleanupHours {
				if !c.dryRun {
					_, err = c.s3.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
						Bucket:   aws.String(c.bucket),
						Key:      multi.Key,
						UploadId: multi.UploadId,
					})

					if err != nil {
						fmt.Printf(" ERROR: %s\n", err)
					} else {
						fmt.Println("   Removed!")
						totalRemoved++
					}
				}
			}
		}

		if aws.BoolValue(resp.IsTruncated) && len(resp.Uploads) > 0 {
			keyMarker = resp.Uploads[len(resp.Uploads)-1].Key
			uploadIDMarker = resp.Uploads[len(resp.Uploads)-1].UploadId
		} else {
			return totalRemoved, nil
		}
	}
}

func (c *S3Cleaner) cleanUploadFolders(prefix string) error {
	shouldContinue := true
	var continuationToken *string
	for shouldContinue {
		objs, err := c.s3.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket:            aws.String(c.bucket),
			Prefix:            aws.String(prefix),
			MaxKeys:           aws.Int64(100),
			ContinuationToken: continuationToken,
		})

		if err != nil {
			return errors.Wrapf(err, "failed to list objects for prefix %s", prefix)
		}

		for _, o := range objs.Contents {
			if strings.Contains(*o.Key, "/_uploads/") && strings.HasSuffix(aws.StringValue(o.Key), "/startedat") {
				hoursSince, err := c.hoursSinceUploadStarted(aws.StringValue(o.Key))
				if err != nil {
					fmt.Printf(" ERROR: %s\n", err)
					continue
				}

				if hoursSince > cleanupHours {
					fmt.Printf("  Removing folder %s (%d hours)\n", aws.StringValue(o.Key), hoursSince)
					if err := c.removeUploadFolder(aws.StringValue(o.Key)); err != nil {
						return errors.Wrap(err, "failed to remove upload folder")
					}
				} else {
					fmt.Printf("  Skipping folder %s (%d hours)\n", aws.StringValue(o.Key), hoursSince)
				}
			}
		}

		continuationToken = objs.NextContinuationToken
		shouldContinue = aws.BoolValue(objs.IsTruncated)
	}

	return nil
}

func (c *S3Cleaner) removeUploadFolder(prefix string) error {
	keyParts := strings.Split(prefix, "/")
	uploadsFolder := strings.Join(keyParts[0:len(keyParts)-1], "/")

	objs, err := c.s3.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(uploadsFolder),
	})

	if err != nil {
		return errors.Wrapf(err, "failed to list objects for prefix %s", uploadsFolder)
	}

	for _, o := range objs.Contents {
		if !c.dryRun {
			_, err := c.s3.DeleteObject(&s3.DeleteObjectInput{
				Bucket: aws.String(c.bucket),
				Key:    o.Key,
			})

			if err != nil {
				return errors.Wrapf(err, "failed to delete object %s", aws.StringValue(o.Key))
			}
		}

		fmt.Printf("    Removing %s\n", aws.StringValue(o.Key))
	}

	return nil
}

func (c *S3Cleaner) hoursSinceUploadStarted(key string) (int, error) {
	obj, err := c.s3.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		return 0, err
	}

	defer obj.Body.Close()
	t, err := parseTimeFromStream(obj.Body)
	if err != nil {
		panic(err)
	}

	return int(time.Since(t).Hours()), nil
}

func getS3Client(endPoint, accessKey, secretAccessKey string, skipTLSVerify bool) *s3.S3 {
	awsConfig := aws.NewConfig()

	creds := credentials.NewChainCredentials([]credentials.Provider{
		&credentials.StaticProvider{
			Value: credentials.Value{
				AccessKeyID:     accessKey,
				SecretAccessKey: secretAccessKey,
			},
		},
		&credentials.EnvProvider{},
		&credentials.SharedCredentialsProvider{},
		&ec2rolecreds.EC2RoleProvider{Client: ec2metadata.New(session.New())},
	})

	if skipTLSVerify {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		awsConfig.WithHTTPClient(&http.Client{Transport: tr})
	}

	awsConfig.WithS3ForcePathStyle(true)
	awsConfig.WithEndpoint(endPoint)

	awsConfig.WithCredentials(creds)
	awsConfig.WithRegion("us-west-1")
	awsConfig.WithDisableSSL(true)

	return s3.New(session.New(awsConfig))
}

func parseTimeFromStream(s io.Reader) (time.Time, error) {
	buf := new(bytes.Buffer)

	_, err := buf.ReadFrom(s)
	if err != nil {
		return time.Time{}, err
	}

	dateString := buf.String()
	return time.Parse(startedadDateFormat, dateString)
}
