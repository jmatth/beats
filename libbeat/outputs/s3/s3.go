package s3out

import (
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/common/op"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/outputs"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var debug = logp.MakeDebug("s3")

func init() {
	outputs.RegisterOutputPlugin("s3", New)
}

// A subset of github.com/aws/aws-sdk-go/blob/master/service/s3/s3iface.S3API
type S3API interface {
	PutObject(*s3.PutObjectInput) (*s3.PutObjectOutput, error)
}

type s3Output struct {
	config      config
	beatName    string
	s3Svc       S3API
	ticker      *time.Ticker
	consumerMap map[string]ConsumerAPI
	consumerWg  *sync.WaitGroup
}

// New instantiates a new s3 output instance.
func New(beatName string, cfg *common.Config, _ int) (outputs.Outputer, error) {
	config := defaultConfig
	if err := cfg.Unpack(&config); err != nil {
		logp.Err("Error unpacking config for s3 output.")
		return nil, err
	}

	if config.AccessKeyId != "" && config.SecretAccessKey != "" {
		debug("Found aws credentials in config, setting environment variables")
		os.Setenv("AWS_ACCESS_KEY_ID", config.AccessKeyId)
		os.Setenv("AWS_SECRET_ACCESS_KEY", config.SecretAccessKey)
	}

	// disable bulk support in publisher pipeline
	cfg.SetInt("flush_interval", -1, -1)
	cfg.SetInt("bulk_max_size", -1, -1)

	svc := s3.New(session.New(&aws.Config{Region: aws.String(config.Region)}))

	output := &s3Output{
		beatName:    beatName,
		s3Svc:       svc,
		consumerMap: make(map[string]ConsumerAPI),
		consumerWg:  &sync.WaitGroup{},
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
	out.startTicker()

	return nil
}

func (out *s3Output) PublishEvent(
	sig op.Signaler,
	opts outputs.Options,
	data outputs.Data,
) (err error) {

	defer func() { op.Sig(sig, err) }()

	appType, err := getAppType(data)
	if err != nil {
		return err
	}

	message, err := getMessage(data)
	if err != nil {
		return err
	}

	consumer, err := out.getConsumer(appType)

	consumer.AppendLine(message)

	return err
}

func (out *s3Output) Close() error {
	debug("Close called on s3 outputter, shutting down")
	out.ticker.Stop()
	for _, consumer := range out.consumerMap {
		consumer.Shutdown()
	}
	out.consumerWg.Wait()
	return nil
}

func (out *s3Output) startTicker() {
	debug("Starting s3 ticker")
	out.ticker = time.NewTicker(time.Second * time.Duration(out.config.SecondsPerChunk))
	go func() {
		for tick := range out.ticker.C {
			debug("Recieved tick in s3 output, signalling consumers")
			for _, consumer := range out.consumerMap {
				consumer.Tick(tick)
			}
		}
	}()
}

func getAppType(data outputs.Data) (string, error) {
	appTypeInterface, err := data.Event.GetValue("fields.appType")
	if err != nil {
		logp.Info("Could not retrieve fields.appType for s3 output. Falling back to basename of source file")
		sourceInterface, err := data.Event.GetValue("source")
		if err != nil {
			logp.Err("Could not get the source of event for s3 output and appType not set, bailing out")
			return "", err
		}
		source := sourceInterface.(string)
		return filepath.Base(source), nil
	} else {
		return appTypeInterface.(string), nil
	}
}

func getMessage(data outputs.Data) (string, error) {
	messageInterface, err := data.Event.GetValue("message")
	if err != nil {
		logp.Err("Could not get message for s3 output. Malformed event?")
		return "", err
	}
	return messageInterface.(string), nil
}

func (out *s3Output) getConsumer(appType string) (ConsumerAPI, error) {
	consumer := out.consumerMap[appType]
	if consumer == nil {
		consumer, err := newConsumer(out.config.TemporaryDirectory, appType, out.s3Svc, out.config.Bucket, out.config.Prefix)
		if err != nil {
			logp.Err("Error creating consumer for appType %v: %v", appType, err)
			return nil, err
		}

		out.consumerMap[appType] = consumer
		out.consumerWg.Add(1)
		go func() {
			defer out.consumerWg.Done()
			consumer.Run()
		}()
	}
	return consumer, nil
}
