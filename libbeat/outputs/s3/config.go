package s3out

import (
	"fmt"
	"os"
	"path/filepath"
)

type config struct {
	AccessKeyId        string `config:"access_key_id"`
	SecretAccessKey    string `config:"secret_access_key"`
	Region             string `config:"region"`
	Bucket             string `config:"bucket"`
	Prefix             string `config:"prefix"`
	TemporaryDirectory string `config:"temporary_directory"`
	SecondsPerChunk    int    `config:"seconds_per_chunk"`
	RetryLimitSeconds  int    `config:"retry_limit_seconds"`
}

var (
	defaultConfig = config{
		Region:             "us-east-1",
		TemporaryDirectory: filepath.Join(os.TempDir(), "beat_s3"),
		SecondsPerChunk:    300,
		RetryLimitSeconds:  60 * 30,
	}
)

func (c *config) Validate() error {
	if c.Bucket == "" {
		return fmt.Errorf("Must specify an s3 bucket")
	}

	if c.SecondsPerChunk < 1 {
		return fmt.Errorf("seconds_per_chunk must be a positive integer")
	}

	return nil
}
