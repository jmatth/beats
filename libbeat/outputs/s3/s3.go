package s3out

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/common/op"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/outputs"
	"github.com/elastic/go-ucfg"

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
	config       config
	s3Svc        S3API
	consumerLock *sync.RWMutex
	consumerMap  map[string]consumerAPI
	consumerWg   *sync.WaitGroup
}

// New instantiates a new s3 output instance.
func New(_ string, cfg *common.Config, _ int) (outputs.Outputer, error) {
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
		s3Svc:        svc,
		consumerLock: new(sync.RWMutex),
		consumerMap:  make(map[string]consumerAPI),
		consumerWg:   &sync.WaitGroup{},
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

func (out *s3Output) PublishEvent(
	sig op.Signaler,
	opts outputs.Options,
	data outputs.Data,
) (err error) {

	defer func() { op.Sig(sig, err) }()

	appType, err := getConsumerOptions(data)
	if err != nil {
		return err
	}

	message, err := getMessage(data)
	if err != nil {
		return err
	}

	consumer, err := out.getConsumer(appType)
	if err != nil {
		return err
	}

	consumer.appendLine(message)

	return err
}

func (out *s3Output) Close() error {
	debug("Close called on s3 outputter, shutting down")
	out.consumerLock.RLock()
	for _, consumer := range out.consumerMap {
		consumer.shutdown()
	}
	out.consumerLock.RUnlock()
	out.consumerWg.Wait()
	return nil
}

func getConsumerOptions(data outputs.Data) (*consumerOptions, error) {
	options := &consumerOptions{}

	appTypeInterface, err := data.Event.GetValue("fields.s3")
	if err == nil {
		config, err := ucfg.NewFrom(appTypeInterface.(common.MapStr))
		if err == nil {
			config.Unpack(options)
		}
	}

	if options.AppType == "" {
		logp.Info("Could not retrieve appType for s3 output, using source for appType instead")
		sourceInterface, err := data.Event.GetValue("source")
		if err != nil {
			logp.Err("Could not get the source of event for s3 output and appType not set, bailing out")
			return nil, err
		}
		source := sourceInterface.(string)
		options.AppType = filepath.Base(source)
	}

	debug("Build consumer options: %v", options)
	return options, nil
}

func getMessage(data outputs.Data) (string, error) {
	messageInterface, err := data.Event.GetValue("message")
	if err != nil {
		logp.Err("Could not get message for s3 output. Malformed event?")
		return "", err
	}
	return messageInterface.(string), nil
}

func (out *s3Output) getConsumer(options *consumerOptions) (consumer consumerAPI, err error) {
	out.consumerLock.RLock()
	consumer = out.consumerMap[options.AppType]
	out.consumerLock.RUnlock()

	if consumer != nil {
		return
	}

	out.consumerLock.Lock()
	defer out.consumerLock.Unlock()
	consumer = out.consumerMap[options.AppType]

	// It is possible that another goroutine got the lock and instantiated
	// a consumer before us, double check now that we have the write lock.
	if consumer != nil {
		return
	}

	consumer, err = newConsumer(out.config, options, out.s3Svc)
	if err != nil {
		logp.Err("Error creating consumer for appType %v: %v", options.AppType, err)
		return
	}

	out.consumerMap[options.AppType] = consumer

	// This WaitGroup is used to wait for all consumers to upload any remaining chunks when Close() is called.
	out.consumerWg.Add(1)
	go func() {
		defer out.consumerWg.Done()
		consumer.run()
	}()

	return
}
