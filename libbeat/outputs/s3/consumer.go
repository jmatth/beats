package s3out

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/elastic/beats/libbeat/logp"
)

type consumer struct {
	lineChan     chan string
	tickChan     chan time.Time
	appType      string
	file         *os.File
	s3Svc        *s3.S3
	bucket       string
	prefix       string
	linesWritten int
}

func (c *consumer) run() {
	debug("Starting s3 consumer for app: %v", c.appType)
	for {
		select {
		case tick := <-c.tickChan:
			c.upload(tick)
		case line, ok := <-c.lineChan:
			if ok {
				c.append(line)
			} else {
				debug("Channel to s3 consumer %v closed, uploading and stopping", c.appType)
				c.upload(time.Now())
				return
			}
		}
	}
}

func (c *consumer) append(line string) {
	fmt.Fprintln(c.file, line)
	c.linesWritten++
}

func (c *consumer) upload(tick time.Time) {

	if c.linesWritten < 1 {
		debug("No lines recieved for %v, not uploading", c.appType)
		return
	}

	debug("Uploading %v to s3 with timestamp %v", c.appType, tick.UTC().Unix())
	err := c.file.Sync()
	if err != nil {
		logp.Err(err.Error())
		return
	}

	reader, err := os.Open(c.file.Name())
	if err != nil {
		logp.Err(err.Error())
		return
	}
	defer reader.Close()

	response, err := c.s3Svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(path.Join(c.prefix, c.appType, strconv.FormatInt(tick.UTC().Unix(), 10))),
		Body:   reader,
	})
	if err != nil {
		logp.Err(err.Error())
		return
	}
	debug(response.String())
	c.linesWritten = 0

	err = c.file.Truncate(0)
	if err != nil {
		logp.Err(err.Error())
		return
	}

	_, err = c.file.Seek(0, 0)
	if err != nil {
		logp.Err(err.Error())
		return
	}

}

func newConsumer(tempDir string, appType string, s3Svc *s3.S3, bucket string, prefix string) (*consumer, error) {
	file, err := os.Create(path.Join(tempDir, appType))
	if err != nil {
		logp.Err("Failed to create temporary file: %v", file.Name())
		return nil, err
	}
	logp.Info("Created new temporary file: %v", file.Name())

	return &consumer{
		lineChan: make(chan string),
		tickChan: make(chan time.Time),
		appType:  appType,
		file:     file,
		s3Svc:    s3Svc,
		bucket:   bucket,
		prefix:   prefix,
	}, nil
}
