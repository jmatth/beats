package s3out

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestEvictOldFiles(t *testing.T) {
	tempDir := mkTempDir(t)
	defer rmTempDir(t, tempDir)
	setupLogp(t)
	testConfig := getTestConfig(tempDir)
	testConfig.RetryLimitSeconds = 0

	blockMockChan := make(chan time.Time)
	s3SvcMock := new(s3Mock)
	s3SvcMock.On("PutObject", mock.AnythingOfType("*s3.PutObjectInput")).Return(&s3.PutObjectOutput{}, nil).Once()
	s3SvcMock.On("PutObject", mock.AnythingOfType("*s3.PutObjectInput")).Return(nil, errors.New("We are investigating increased  API error rates in the US-EAST-1 Region.")).WaitUntil(blockMockChan)

	uploader := newS3Uploader(testConfig, "testEvectOldFiles", s3SvcMock)
	// Needed to avoid deadlock at the end of the test
	uploader.fileChan = make(chan *os.File, 1)

	files := make([]*os.File, 4)
	for i := 0; i < 4; i++ {
		file, err := os.Create(filepath.Join(tempDir, fmt.Sprintf("file%v", i)))
		if err != nil || file == nil {
			t.Logf("%v; %v", file, err)
			t.FailNow()
		}
		files[i] = file
	}

	files[0].WriteString("One file")
	files[1].WriteString("Two file")
	files[2].WriteString("Red file")
	files[3].WriteString("Blue file")

	go uploader.recieveAndUpload()

	debug("Sending first file")
	uploader.fileChan <- files[0]

	debug("Sending second file")
	uploader.fileChan <- files[1]
	debug("Allowing second api call to fail")
	blockMockChan <- time.Now()

	debug("Sending third file")
	uploader.retryLimit = time.Hour
	uploader.fileChan <- files[2]

	debug("Allowing third api call to fail")
	blockMockChan <- time.Now()

	debug("Sending fourth file")
	uploader.fileChan <- files[3]

	uploader.shutdown()

	// The first file should be deleted since it uploaded
	_, err := os.Stat(files[0].Name())
	assert.True(t, os.IsNotExist(err))

	// The second file should be deleted since it timed out
	_, err = os.Stat(files[1].Name())
	assert.True(t, os.IsNotExist(err))

	// The third should be left on disk since we shutdown before it timed out
	_, err = os.Stat(files[2].Name())
	assert.Nil(t, err)

	// The fourth file should be left on disk and we shouldn't have even tried to
	// upload it since the previous one failed and shutdown was called
	_, err = os.Stat(files[3].Name())
	assert.Nil(t, err)
	s3SvcMock.AssertNumberOfCalls(t, "PutObject", 3)
}
