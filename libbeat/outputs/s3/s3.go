package s3out

import (
	"fmt"
	"os"
	"path"

	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/common/op"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/outputs"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func init() {
	outputs.RegisterOutputPlugin("s3", New)
}

type s3Output struct {
	config   config
	beatName string
	s3Svc    *s3.S3
	fileMap  map[string]*os.File
}

// New instantiates a new s3 output instance.
func New(beatName string, cfg *common.Config, _ int) (outputs.Outputer, error) {
	config := defaultConfig
	if err := cfg.Unpack(&config); err != nil {
		logp.Err("Error unpacking config for s3 output.")
		return nil, err
	}

	// disable bulk support in publisher pipeline
	cfg.SetInt("flush_interval", -1, -1)
	cfg.SetInt("bulk_max_size", -1, -1)

	svc := s3.New(session.New(&aws.Config{Region: aws.String(config.Region)}))

	output := &s3Output{
		beatName: beatName,
		s3Svc:    svc,
		fileMap:  make(map[string]*os.File),
	}

	if err := output.init(config); err != nil {
		logp.Err("Error calling init for s3 output.")
		return nil, err
	}

	return output, nil

}

func (out *s3Output) init(config config) error {
	out.config = config
	tempDir := out.config.TemporaryDirectory
	if err := os.MkdirAll(tempDir, 0700); err != nil {
		logp.Err("Failed to create s3 temporary file directory: %v", tempDir)
		return err
	}
	logp.Info("Created directory for temporary s3 files: %v", tempDir)

	return nil
}

// Implement Outputer
func (out *s3Output) Close() error {
	return nil
}

func (out *s3Output) PublishEvent(
	sig op.Signaler,
	opts outputs.Options,
	data outputs.Data,
) (err error) {

	defer func() { op.Sig(sig, err) }()

	appTypeInterface, err := data.Event.GetValue("fields.appType")
	if err != nil {
		logp.Err("Could not retrieve fields.appType for s3 output. Missing configuration?")
		return err
	}
	appType := appTypeInterface.(string)
	file := out.fileMap[appType]

	if file == nil {
		filePath := path.Join(out.config.TemporaryDirectory, appType)
		file, err = os.Create(filePath)
		if err != nil {
			logp.Err("Failed to create temporary file: %v", filePath)
			return err
		}
		out.fileMap[appType] = file
		logp.Info("Created new temporary file: %v", filePath)
	}

	messageInterface, err := data.Event.GetValue("message")
	if err != nil {
		logp.Err("Could not get message for s3 output. Malformed event?")
		return err
	}
	message := messageInterface.(string)
	_, err = fmt.Fprintln(file, message)
	if err != nil {
		logp.Err("Could not unwrap message for s3 output.")
	}

	return err
}
