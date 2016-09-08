package s3out

import (
	"io/ioutil"
	"os"
	"testing"

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
