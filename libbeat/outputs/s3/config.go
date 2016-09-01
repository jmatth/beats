package s3out

import (
	"os"
	"path"
)

type config struct {
	AccessKeyId        string `config:access_key_id`
	SecretAccessKey    string `config:secret_access_key`
	AwsCredentialsFile string `config:aws_credentials_file`
	Region             string `config:region`
	Bucket             string `config:bucket`
	TemporaryDirectory string `config:temporary_directory`

	// RotateEveryKb int    `config:"rotate_every_kb" validate:"min=1"`
}

var (
	defaultConfig = config{
		AwsCredentialsFile: path.Join(os.Getenv("HOME"), "aws", "credentials"),
		Region:             "us-east-1",
		TemporaryDirectory: "/tmp/beat_s3/",
	}
)

func (c *config) Validate() error {
	// if c.NumberOfFiles < 2 || c.NumberOfFiles > logp.RotatorMaxFiles {
	// 	return fmt.Errorf("The number_of_files to keep should be between 2 and %v",
	// 		logp.RotatorMaxFiles)
	// }

	return nil
}
