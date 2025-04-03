package storage

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"S3RunFast/pkg/common"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3Client struct {
	client *s3.S3
	config *common.S3Config
}

func NewS3Client(config *common.S3Config) (*S3Client, error) {
	creds := credentials.NewStaticCredentials(
		config.AccessKey,
		config.SecretKey,
		"",
	)

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region:           aws.String(config.Region),
			Endpoint:         aws.String(config.Endpoint),
			S3ForcePathStyle: aws.Bool(config.ForcePathStyle),
			Credentials:      creds,
		},
	}))

	return &S3Client{
		client: s3.New(sess),
		config: config,
	}, nil
}

// ListBuckets 列出所有桶
func (c *S3Client) ListBuckets() ([]string, error) {
	result, err := c.client.ListBuckets(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to list buckets: %v", err)
	}

	var buckets []string
	for _, bucket := range result.Buckets {
		buckets = append(buckets, *bucket.Name)
	}
	return buckets, nil
}

// CreateBucket 创建桶（幂等操作）
func (c *S3Client) CreateBucket(bucket string) error {
	input := &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
		CreateBucketConfiguration: &s3.CreateBucketConfiguration{
			LocationConstraint: aws.String(""),
		},
	}

	_, err := c.client.CreateBucket(input)
	if err != nil {
		// 如果桶已存在，返回成功
		if strings.Contains(err.Error(), "BucketAlreadyExists") {
			return nil
		}
		return fmt.Errorf("failed to create bucket %s: %v", bucket, err)
	}
	return nil
}

// ListObjects 列出桶中的所有对象
func (c *S3Client) ListObjects(bucket string, marker *string) (*s3.ListObjectsOutput, error) {
	input := &s3.ListObjectsInput{
		Bucket:  aws.String(bucket),
		Marker:  marker,
		MaxKeys: aws.Int64(1000),
	}

	return c.client.ListObjects(input)
}

// GetObject 获取对象内容
func (c *S3Client) GetObject(bucket, key string) (*s3.GetObjectOutput, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	return c.client.GetObject(input)
}

// PutObject 上传对象
func (c *S3Client) PutObject(bucket, key string, body io.ReadSeeker) error {
	input := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   body,
	}

	_, err := c.client.PutObject(input)
	if err != nil {
		return fmt.Errorf("failed to put object %s/%s: %v", bucket, key, err)
	}
	return nil
}

// GetObjectLastModified 获取对象的最后修改时间
func (c *S3Client) GetObjectLastModified(bucket, key string) (time.Time, error) {
	input := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	result, err := c.client.HeadObject(input)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get object last modified time: %v", err)
	}

	return *result.LastModified, nil
}

// HeadObject retrieves metadata from an object without returning the object itself.
func (c *S3Client) HeadObject(bucket, key string) (*s3.HeadObjectOutput, error) {
	input := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	result, err := c.client.HeadObject(input)
	if err != nil {
		return nil, fmt.Errorf("failed to head object %s/%s: %v", bucket, key, err)
	}

	return result, nil
}

// UploadLargeObject 分片上传大文件
func (c *S3Client) UploadLargeObject(bucket, key string, filePath string, chunkSize int64) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	// 创建分片上传
	createResp, err := c.client.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to create multipart upload: %v", err)
	}

	var completedParts []*s3.CompletedPart
	partNumber := int64(1)
	buffer := make([]byte, chunkSize)

	for {
		n, err := file.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read file: %v", err)
		}

		// 上传分片
		uploadResp, err := c.client.UploadPart(&s3.UploadPartInput{
			Bucket:        aws.String(bucket),
			Key:           aws.String(key),
			PartNumber:    aws.Int64(partNumber),
			UploadId:      createResp.UploadId,
			Body:          bytes.NewReader(buffer[:n]),
			ContentLength: aws.Int64(int64(n)),
		})
		if err != nil {
			return fmt.Errorf("failed to upload part: %v", err)
		}

		completedParts = append(completedParts, &s3.CompletedPart{
			ETag:       uploadResp.ETag,
			PartNumber: aws.Int64(partNumber),
		})

		partNumber++
	}

	// 完成分片上传
	_, err = c.client.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: createResp.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to complete multipart upload: %v", err)
	}

	return nil
}

// DownloadLargeObject 分片下载大文件
func (c *S3Client) DownloadLargeObject(bucket, key string, filePath string) error {
	// 确保目录存在
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return fmt.Errorf("failed to create directory: %v", err)
	}

	// 创建临时文件
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %v", err)
	}
	defer file.Close()

	// 获取对象大小
	headResp, err := c.client.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to get object info: %v", err)
	}

	// 分片下载
	rangeStart := int64(0)
	rangeEnd := *headResp.ContentLength - 1
	chunkSize := int64(5 * 1024 * 1024) // 5MB chunks

	for rangeStart < *headResp.ContentLength {
		if rangeEnd-rangeStart+1 > chunkSize {
			rangeEnd = rangeStart + chunkSize - 1
		}

		getResp, err := c.client.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Range:  aws.String(fmt.Sprintf("bytes=%d-%d", rangeStart, rangeEnd)),
		})
		if err != nil {
			return fmt.Errorf("failed to download chunk: %v", err)
		}

		if _, err := io.Copy(file, getResp.Body); err != nil {
			return fmt.Errorf("failed to write chunk to file: %v", err)
		}

		rangeStart = rangeEnd + 1
		rangeEnd = *headResp.ContentLength - 1
	}

	return nil
}
