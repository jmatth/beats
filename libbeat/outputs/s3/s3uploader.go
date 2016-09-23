package s3out

import (
	"errors"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/elastic/beats/libbeat/logp"
)

const retryInterval = time.Second * 30

type s3uploader struct {
	fileChan     chan *os.File
	shutdownChan chan bool
	retryLimit   time.Duration
	appType      string
	bucket       string
	prefix       string
	s3Svc        S3API
}

func newS3Uploader(c config, appType string, s3Svc S3API) *s3uploader {
	retryLimit := time.Minute * time.Duration(c.RetryLimitSeconds)
	uploadInterval := time.Second * time.Duration(c.SecondsPerChunk)
	channelSize := int64(retryLimit / uploadInterval)
	debug("computed channel size to be %v; uploadInterval: %v, retryLimit: %v", channelSize, uploadInterval, retryLimit)

	return &s3uploader{
		fileChan:     make(chan *os.File, channelSize),
		shutdownChan: make(chan bool),
		retryLimit:   retryLimit,
		appType:      appType,
		bucket:       c.Bucket,
		prefix:       c.Prefix,
		s3Svc:        s3Svc,
	}
}

func (s *s3uploader) shutdown() {
	close(s.fileChan)
	close(s.shutdownChan)
}

func (s *s3uploader) recieveAndUpload() {
	debug("recieveAndUpload goroutine started for s3uploader")

	for {
		// Wait until we have something to do.
		f, ok := <-s.fileChan
		if !ok {
			debug("recieveAndUpload goroutine exiting")
			break
		}

		if err := s.tryUpload(f); err != nil {
			logp.Err("tryUpload returned an error, shutting down s3 uploader goroutine. (%v)", err)
			break
		}
	}
}

func (s *s3uploader) tryUpload(file *os.File) error {
	tryUntil := time.Now().Add(s.retryLimit)
	for {

		err := s.s3Put(file)
		if err == nil {
			removeFile(file)
			break
		}

		now := time.Now()
		if now.Add(retryInterval).After(tryUntil) {
			logp.Err("Failed to upload %v for too long, dropping the chunk", file.Name())
			removeFile(file)
			break
		}

		logp.Err("Failed to upload %v, will try again in %v and give up in %v", file.Name(), retryInterval, tryUntil.Sub(now))
		select {
		case <-s.shutdownChan:
			return errors.New("S3 upload failed during shutdown, abandoning current and future uploads. We will try to recover them on the next run.")
		case <-time.After(retryInterval):
		}
	}

	return nil
}

func (s *s3uploader) s3Put(file *os.File) error {

	fInfo, err := file.Stat()
	if err != nil {
		return err
	}

	_, err = file.Seek(0, 0)
	if err != nil {
		return err
	}

	timeStamp := strconv.FormatInt(fInfo.ModTime().UTC().Unix(), 10)

	debug("Uploading %v to s3", fInfo.Name())
	response, err := s.s3Svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path.Join(s.prefix, s.appType, timeStamp)),
		Body:   file,
	})
	if err != nil {
		return err
	}
	debug(response.String())

	return nil
}
