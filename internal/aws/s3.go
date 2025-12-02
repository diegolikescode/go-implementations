package aws

import (
	"context"
	"errors"
	"io"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/diegolikescode/go-data-streaming/internal/metrics"
)

type S3Stream interface {
	StreamToBucket(ctx context.Context, destKey string, readerFunc func(r io.Reader) error) error
	StreamLocalFileToBucket(ctx context.Context, localPath, destKey string, transformFunc func(r io.Reader, w io.Writer) error) error
	StreamWithTransform(ctx context.Context, source io.Reader, destKey string, transformFunc func(r io.Reader, w io.Writer) error) error
}

type S3Manager struct {
	BucketName  string
	Dir         string
	s3Client    *s3.Client
	uploader    *manager.Uploader
	memObserver *metrics.MemObserver
}

func setupS3Manager() *s3.Client {
	key := os.Getenv("KEY")
	secret := os.Getenv("SHIESH")
	region := os.Getenv("AWS_REGION")

	if key == "" || secret == "" {
		panic(errors.New("key and/or secret not set propperly"))
	}

	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(region))
	if err != nil {
		panic(errors.New("error in setup AWS config"))
	}

	return s3.NewFromConfig(cfg)
}

func NewS3Manager(dir string) *S3Manager {
	s3Client := setupS3Manager()
	bucketName := os.Getenv("S3_BUCKET_NAME")
	mem := metrics.NewMemObserver()
	uploader := manager.NewUploader(s3Client, func(u *manager.Uploader) {
		u.PartSize = bToMb(5)
		u.Concurrency = 1
		u.BufferProvider = manager.NewBufferedReadSeekerWriteToPool(1024 * 1024)
	})

	return &S3Manager{BucketName: bucketName, Dir: dir, s3Client: s3Client, uploader: uploader, memObserver: mem}
}

func bToMb(b uint64) int64 {
	return int64(b * 1024 * 1024)
}

func (m *S3Manager) StreamToBucket(ctx context.Context, destKey string, readerFunc func(r io.Reader) error) error {
	return nil
}

func (m *S3Manager) StreamLocalFileToBucket(ctx context.Context, localPath, destKey string, transformFunc func(r io.ReadCloser, w io.Writer) error) error {
	m.memObserver.PrintMemoryUsage("start opening file")
	f, err := os.Open(localPath)
	if err != nil {
		log.Printf("err while trying to open file: %v", err)
	}

	pipeReader, pipeWriter := io.Pipe()
	defer pipeReader.Close()
	go func() {
		defer pipeWriter.Close()
		m.memObserver.PrintMemoryUsage("start transformFunc")
		transformFunc(f, pipeWriter)
	}()

	m.memObserver.PrintMemoryUsage("start uploader")
	_, err = m.uploader.Upload(context.Background(), &s3.PutObjectInput{Bucket: &m.BucketName, Key: &destKey, Body: pipeReader})
	if err != nil {
		log.Fatalf("err SENDING the object: %v", err)
	}

	m.memObserver.PrintMemoryUsage("file fully sent into S3")

	return nil
}

func (m *S3Manager) StreamWithTransform(ctx context.Context, source io.Reader, destKey string, transformFunc func(r io.Reader, w io.WriteCloser) error) error {
	m.memObserver.PrintMemoryUsage("start streaming inmemory content")

	pipeReader, pipeWriter := io.Pipe()
	defer pipeReader.Close()
	go func() {
		defer pipeWriter.Close()
		transformFunc(source, pipeWriter)
	}()

	m.memObserver.PrintMemoryUsage("start uploading")
	_, err := m.uploader.Upload(context.Background(), &s3.PutObjectInput{Bucket: &m.BucketName, Key: &destKey, Body: pipeReader})
	if err != nil {
		log.Printf("Err while uploading the file: %v", err)
		return err
	}

	return nil
}
