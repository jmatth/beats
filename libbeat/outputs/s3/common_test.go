package s3out

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/elastic/beats/libbeat/logp"
	"github.com/stretchr/testify/assert"
)

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

func setupLogp(t *testing.T) {
	if testing.Verbose() {
		logp.LogInit(logp.LOG_DEBUG, "", false, true, []string{"s3"})
	}
}

func getTestConfig(tempDir string) config {
	return config{
		AccessKeyId:        "testKeyId",
		SecretAccessKey:    "testSecretKey",
		Region:             "US-EAST-1",
		Bucket:             "testBucket",
		Prefix:             "testPrefix/",
		TemporaryDirectory: tempDir,
		SecondsPerChunk:    60 * 60 * 2,
		RetryLimitSeconds:  60 * 60,
	}
}
