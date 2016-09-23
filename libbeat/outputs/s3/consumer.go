package s3out

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/elastic/beats/libbeat/logp"
)

type consumerAPI interface {
	run()
	appendLine(string)
	shutdown()
}

type consumer struct {
	lineChan         chan string
	ticker           *time.Ticker
	chunkDuration    time.Duration
	chunkStartTime   time.Time
	appType          string
	timestampRegex   *regexp.Regexp
	timestampFormat  string
	baseFilePath     string
	file             *os.File
	uploader         *s3uploader
	uploadThreadChan chan bool
}

type consumerOptions struct {
	AppType         string `config:"appType"`
	TimestampRegex  string `config:"timestampRegex"`
	TimestampFormat string `config:"timestampFormat"`
}

func (c *consumer) appendLine(line string) {
	c.lineChan <- line
}

func (c *consumer) shutdown() {
	close(c.lineChan)
}

func (c *consumer) run() {

	debug("running consumer for app: %v", c.appType)

	for {
		select {
		case <-c.ticker.C:
			c.upload(true)
		case line, ok := <-c.lineChan:
			if ok {
				c.append(line)
			} else {
				c.upload(false)
				c.uploader.shutdown()
				logp.Info("Waiting for s3 uploads for %v to complete...", c.appType)
				<-c.uploadThreadChan
				return
			}
		}
	}
}

func (c *consumer) append(line string) {
	timestamp, err := c.getLineTimestamp(line)
	if err != nil {
		logp.Err("%v", err)
	}

	if timestamp != nil {
		if timestamp.Before(c.chunkStartTime) || timestamp.After(c.chunkStartTime.Add(c.chunkDuration)) {
			c.upload(true)
			c.chunkStartTime = *timestamp
		}
	}

	fmt.Fprintln(c.file, line)

	if timestamp != nil {
		c.setModTime(c.file.Name(), *timestamp)
	}
}

func (c *consumer) getLineTimestamp(line string) (*time.Time, error) {
	if c.timestampRegex == nil {
		return nil, nil
	}

	timestampStr := c.timestampRegex.FindString(line)
	if timestampStr == "" {
		return nil, errors.New(fmt.Sprintf("Could not find a timestamp in line for %v: %v", c.appType, line))
	}

	timestamp, err := time.Parse(c.timestampFormat, timestampStr)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error parsing timestamp: %v", err))
	}

	return &timestamp, nil
}

func (c *consumer) setModTime(filePath string, timestamp time.Time) {
	err := os.Chtimes(filePath, timestamp, timestamp)
	if err != nil {
		logp.Err("Error setting timestamp on %v: %v", filePath, err)
	}
}

func (c *consumer) upload(createNewFile bool) {

	fInfo, err := c.file.Stat()
	if err != nil {
		logp.Err("Error retrieving file info: %v", err)
		return
	}

	if fInfo.Size() < 1 {
		logp.Info("Chunk %v is empty, not uploading", c.file.Name())
		return
	}

	err = c.file.Sync()
	if err != nil {
		logp.Err(err.Error())
		return
	}

	debug("Sending %v to uploader goroutine", c.file.Name())
	c.uploader.fileChan <- c.file

	if createNewFile {
		c.createTempFile()
	}

}

func (c *consumer) runUploader() {
	go func() {
		c.uploader.recieveAndUpload()
		debug("recieveAndUpload returned, signalling run()")
		close(c.uploadThreadChan)
	}()
}

func (c *consumer) init() error {
	c.runUploader()
	if err := c.handleLeftoverChunks(); err != nil {
		return err
	}
	if err := c.createTempFile(); err != nil {
		return err
	}
	return nil
}

func (c *consumer) createTempFile() error {
	tempFilePath := fmt.Sprintf("%s_%d", c.baseFilePath, time.Now().UTC().UnixNano())
	file, err := os.Create(tempFilePath)
	if err != nil {
		logp.Err("Failed to create temporary file: %v", tempFilePath)
		return err
	}
	logp.Info("Created new temporary file: %v", file.Name())
	c.file = file
	return nil
}

func (c *consumer) handleLeftoverChunks() error {
	chunkPaths, err := filepath.Glob(fmt.Sprintf("%s_*", c.baseFilePath))
	if err != nil {
		return err
	}

	for _, filePath := range chunkPaths {
		file, err := os.Open(filePath)
		if err != nil {
			logp.Err("Encountered error while accessing leftover chunk %v: %v", filePath, err.Error())
			continue
		}

		fInfo, err := file.Stat()
		if err != nil {
			logp.Err(err.Error())
		}

		if fInfo.Size() < 1 {
			// It's empty, just delete it and move on
			os.Remove(filePath)
			continue
		}

		logp.Info("Found non-empty leftover chunk for %v, uploading it", c.appType)
		// Put it directly in the upload queue, from here on it behaves like a chunk that failed to upload during the current exucution of the program
		c.uploader.fileChan <- file
	}

	return nil
}

func removeFile(file *os.File) {
	debug("Removing file %v", file.Name())
	err := file.Close()
	if err != nil {
		logp.Err("Error closing file: %v", err)
	}
	err = os.Remove(file.Name())
	if err != nil {
		logp.Err("Error removing file %v: %v", file.Name(), err)
	}
}

func newConsumer(c config, options *consumerOptions, s3Svc S3API) (*consumer, error) {
	baseFilePath := filepath.Join(c.TemporaryDirectory, options.AppType)

	newConsumer := &consumer{
		lineChan:         make(chan string),
		ticker:           time.NewTicker(time.Second * time.Duration(c.SecondsPerChunk)),
		chunkDuration:    time.Second * time.Duration(c.SecondsPerChunk),
		chunkStartTime:   time.Now(),
		appType:          options.AppType,
		timestampFormat:  options.TimestampFormat,
		baseFilePath:     baseFilePath,
		uploader:         newS3Uploader(c, options.AppType, s3Svc),
		uploadThreadChan: make(chan bool),
	}
	var err error
	if options.TimestampRegex != "" {
		if options.TimestampFormat == "" {
			logp.Err("timestampRegex specified without timestampFormat")
			return nil, errors.New("Must specify timestampFormat with timestampRegex for s3 output")
		}
		newConsumer.timestampRegex, err = regexp.Compile(options.TimestampRegex)
		if err != nil {
			logp.Err("failed to initialize s3 consumer for %v", options.AppType)
			return nil, err
		}
	}

	err = newConsumer.init()
	if err != nil {
		logp.Err("failed to initialize s3 consumer for %v", options.AppType)
		return nil, err
	}

	return newConsumer, nil
}
