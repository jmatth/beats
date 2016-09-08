// +build !integration

package s3out

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type S3Mock struct {
	mock.Mock
}

func (s *S3Mock) PutObject(obj *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	s.Called(obj)
	return new(s3.PutObjectOutput), nil
}

func mkTempDir(t *testing.T) string {
	tempDir, err := ioutil.TempDir("", "testConsumer")
	assert.Nil(t, err)
	err = os.MkdirAll(tempDir, 0700)
	assert.Nil(t, err)
	t.Logf("Created temporary directory %v", tempDir)
	return tempDir
}

func rmTempDir(t *testing.T, tempDir string) {
	t.Logf("Removing temporary directory %v", tempDir)
	err := os.RemoveAll(tempDir)
	assert.Nil(t, err)
}

func TestShutdown(t *testing.T) {
	tempDir := mkTempDir(t)
	defer func() { rmTempDir(t, tempDir) }()

	consumer, err := newConsumer(tempDir, "testLog", nil, "testBucket", "")
	assert.Nil(t, err)
	assert.NotNil(t, consumer)

	consumerShutdown := make(chan bool)
	go func(consumerShutdown chan<- bool) {
		consumer.Run()
		consumerShutdown <- true
	}(consumerShutdown)

	consumer.Tick(time.Now())
	select {
	case <-consumerShutdown:
		t.Error("Consumer shutdown before Shutdown was called")
	default:
	}

	consumer.Shutdown()

	select {
	case <-consumerShutdown:
	case <-time.After(time.Second * 5):
		t.Error("Consumer failed to shutdown after Shutdown was called")
	}
}

// Make sure we don't upload empty chunks to S3
func TestEmptyChunk(t *testing.T) {
	tempDir := mkTempDir(t)
	defer func() { rmTempDir(t, tempDir) }()

	s3 := new(S3Mock)
	s3.On("PutObject", mock.AnythingOfType("*s3.PutObjectInput")).Return(nil)

	consumer, err := newConsumer(tempDir, "testLog", s3, "testBucket", "")
	assert.Nil(t, err)
	assert.NotNil(t, consumer)

	consumerShutdown := make(chan bool)
	go func(consumerShutdown chan<- bool) {
		consumer.Run()
		consumerShutdown <- true
	}(consumerShutdown)

	select {
	case <-consumerShutdown:
		t.Error("Consumer shutdown before Shutdown was called")
	default:
	}

	consumer.Tick(time.Now())
	// This will block until it's finished uploading and is waiting for another tick
	consumer.Tick(time.Now())

	s3.AssertNotCalled(t, "PutObject", mock.AnythingOfType("*s3.PutObjectInput"))
	consumer.Shutdown()
}

func TestUploadChunk(t *testing.T) {
	tempDir := mkTempDir(t)
	defer func() { rmTempDir(t, tempDir) }()

	s3 := new(S3Mock)
	s3.On("PutObject", mock.AnythingOfType("*s3.PutObjectInput")).Return(nil)

	consumer, err := newConsumer(tempDir, "testLog", s3, "testBucket", "")
	assert.Nil(t, err)
	assert.NotNil(t, consumer)

	consumerShutdown := make(chan bool)
	go func(consumerShutdown chan<- bool) {
		consumer.Run()
		consumerShutdown <- true
	}(consumerShutdown)

	consumer.Tick(time.Now())
	select {
	case <-consumerShutdown:
		t.Error("Consumer shutdown before Shutdown was called")
	default:
	}

	consumer.AppendLine("a log line")
	consumer.Tick(time.Now())
	// This will block until it's finished uploading and is waiting for another tick
	consumer.Tick(time.Now())

	s3.AssertCalled(t, "PutObject", mock.AnythingOfType("*s3.PutObjectInput"))
	s3.AssertNumberOfCalls(t, "PutObject", 1)
	consumer.Shutdown()
}
