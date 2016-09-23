// +build !integration

package s3out

import (
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func getTestconsumerOptions(appType string) *consumerOptions {
	return &consumerOptions{AppType: appType}
}

func Testshutdown(t *testing.T) {
	tempDir := mkTempDir(t)
	defer rmTempDir(t, tempDir)
	setupLogp(t)
	config := getTestConfig(tempDir)

	s3SvcMock := new(s3Mock)
	s3SvcMock.On("PutObject", mock.AnythingOfType("*s3.PutObjectInput")).Return(&s3.PutObjectOutput{}, nil)

	consumer, err := newConsumer(config, getTestconsumerOptions("testLog"), s3SvcMock)
	assert.Nil(t, err)
	assert.NotNil(t, consumer)

	consumershutdown := make(chan bool)
	go func(consumershutdown chan<- bool) {
		consumer.run()
		consumershutdown <- true
	}(consumershutdown)

	select {
	case <-consumershutdown:
		t.Error("Consumer shutdown before shutdown was called")
	default:
	}

	consumer.appendLine("a log line")
	consumer.shutdown()

	select {
	case <-consumershutdown:
	case <-time.After(time.Second * 5):
		t.Error("Consumer failed to shutdown after shutdown was called")
	}

	// Make sure we upload any remaining data before shutting down
	s3SvcMock.AssertCalled(t, "PutObject", mock.AnythingOfType("*s3.PutObjectInput"))
	s3SvcMock.AssertNumberOfCalls(t, "PutObject", 1)
}

// Make sure we don't upload empty chunks to S3
func TestEmptyChunk(t *testing.T) {
	tempDir := mkTempDir(t)
	defer rmTempDir(t, tempDir)
	config := getTestConfig(tempDir)

	s3SvcMock := new(s3Mock)
	s3SvcMock.On("PutObject", mock.AnythingOfType("*s3.PutObjectInput")).Return(&s3.PutObjectOutput{}, nil)

	consumer, err := newConsumer(config, getTestconsumerOptions("testLog"), s3SvcMock)
	assert.Nil(t, err)
	assert.NotNil(t, consumer)

	consumershutdown := make(chan bool)
	go func(consumershutdown chan<- bool) {
		consumer.run()
		consumershutdown <- true
	}(consumershutdown)

	select {
	case <-consumershutdown:
		t.Error("Consumer shutdown before shutdown was called")
	default:
	}

	consumer.upload(true)

	s3SvcMock.AssertNotCalled(t, "PutObject", mock.AnythingOfType("*s3.PutObjectInput"))
	consumer.shutdown()
}

func TestHandleLeftoverChunk(t *testing.T) {
	tempDir := mkTempDir(t)
	defer rmTempDir(t, tempDir)
	setupLogp(t)
	config := getTestConfig(tempDir)

	s3SvcMock := new(s3Mock)
	s3SvcMock.On("PutObject", mock.AnythingOfType("*s3.PutObjectInput")).Return(&s3.PutObjectOutput{}, nil)

	consumer, err := newConsumer(config, getTestconsumerOptions("testLog"), s3SvcMock)
	assert.Nil(t, err)
	assert.NotNil(t, consumer)

	consumer.append("a log line")

	fInfo, err := os.Stat(consumer.file.Name())
	assert.Nil(t, err)

	// The new consumer should find the old file and upload it as is, using the
	// file's last modified timestamp
	secondConsumer, err := newConsumer(config, getTestconsumerOptions("testLog"), s3SvcMock)
	assert.Nil(t, err)
	assert.NotNil(t, secondConsumer)
	secondConsumer.shutdown()
	secondConsumer.run()

	s3SvcMock.AssertCalled(t, "PutObject", mock.AnythingOfType("*s3.PutObjectInput"))
	s3SvcMock.AssertNumberOfCalls(t, "PutObject", 1)
	putData := s3SvcMock.Calls[0].Arguments[0].(*s3.PutObjectInput)
	assert.Equal(t, strconv.FormatInt(fInfo.ModTime().UTC().Unix(), 10), path.Base(*putData.Key))
	consumer.shutdown()
}

func TestGetLineTimestamp(t *testing.T) {
	tempDir := mkTempDir(t)
	defer rmTempDir(t, tempDir)
	setupLogp(t)
	config := getTestConfig(tempDir)

	timeFormat := "2006-01-02T15:04:05.000-0700"
	options := getTestconsumerOptions("testLog")

	logLine := "2016-09-20T14:59:14.736-0400 I NETWORK  [conn1] end connection 127.0.0.1:62218 (0 connections now open)"
	logTime, err := time.Parse(timeFormat, strings.Split(logLine, " ")[0])
	assert.Nil(t, err)

	consumer, err := newConsumer(config, options, nil)
	assert.Nil(t, err)

	// shouldn't do anything because we don't have a regex
	ts, err := consumer.getLineTimestamp(logLine)
	assert.Nil(t, err)
	assert.Nil(t, ts)

	// Should return an error because we have a regex without a format
	options.TimestampRegex = "^[[:digit:]]{4}-[[:digit:]]{2}-[[:digit:]]{2}T[[:digit:]]{2}\\:[[:digit:]]{2}\\:[[:digit:]]{2}\\.[[:digit:]]{3}[+-][[:digit:]]{4}"
	consumer, err = newConsumer(config, options, nil)
	assert.NotNil(t, err)

	// Should modify the timestamp to be in the past
	options.TimestampFormat = timeFormat
	consumer, err = newConsumer(config, options, nil)
	assert.Nil(t, err)
	ts, err = consumer.getLineTimestamp(logLine)
	assert.Nil(t, err)
	assert.NotNil(t, ts)
	assert.True(t, ts.Equal(logTime))
}
