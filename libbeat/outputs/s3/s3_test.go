// +build !integration

package s3out

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/outputs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestOutputInit(t *testing.T) {
	tempDir := mkTempDir(t)
	defer rmTempDir(t, tempDir)
	outputTempDir := filepath.Join(tempDir, "s3Out")

	output := &s3Output{
		beatName:    "beatName",
		s3Svc:       nil,
		consumerMap: make(map[string]ConsumerAPI),
		consumerWg:  &sync.WaitGroup{},
	}

	config := defaultConfig
	config.TemporaryDirectory = outputTempDir

	err := output.init(config)
	assert.Nil(t, err)

	fInfo, err := os.Stat(outputTempDir)
	assert.Nil(t, err)
	assert.True(t, fInfo.IsDir())
}

func TestGetConsumer(t *testing.T) {
	tempDir := mkTempDir(t)
	defer rmTempDir(t, tempDir)
	outputTempDir := filepath.Join(tempDir, "s3Out")

	output := &s3Output{
		beatName:    "beatName",
		s3Svc:       nil,
		consumerMap: make(map[string]ConsumerAPI),
		consumerWg:  &sync.WaitGroup{},
	}

	config := defaultConfig
	config.TemporaryDirectory = outputTempDir

	err := output.init(config)
	assert.Nil(t, err)

	appType := "myApp"
	myConsumerMock := new(consumerMock)
	output.consumerMap[appType] = myConsumerMock
	outConsumer, err := output.getConsumer(appType)
	assert.Nil(t, err)
	assert.Equal(t, outConsumer, myConsumerMock)

	otherApp := "otherApp"
	realConsumer, err := output.getConsumer(otherApp)
	realConsumer.Shutdown()
	assert.Nil(t, err)
	assert.NotNil(t, realConsumer)
}

func TestGetMessage(t *testing.T) {
	myMessage := "some message"
	data := outputs.Data{
		Event: common.MapStr{},
	}

	ret, err := getMessage(data)
	assert.NotNil(t, err)

	data.Event["message"] = myMessage
	ret, err = getMessage(data)
	assert.Nil(t, err)
	assert.Equal(t, myMessage, ret)
}

func TestGetAppType(t *testing.T) {
	data := outputs.Data{
		Event: common.MapStr{},
	}

	ret, err := getAppType(data)
	assert.NotNil(t, err)

	data.Event["source"] = filepath.Join("var", "log", "myApp.log")
	ret, err = getAppType(data)
	assert.Nil(t, err)
	assert.Equal(t, "myApp.log", ret)

	data.Event["fields"] = common.MapStr{}
	data.Event["fields"].(common.MapStr)["appType"] = "myApp"

	ret, err = getAppType(data)
	assert.Nil(t, err)
	assert.Equal(t, "myApp", ret)
}

func TestPublishEvent(t *testing.T) {
	tempDir := mkTempDir(t)
	defer rmTempDir(t, tempDir)
	outputTempDir := filepath.Join(tempDir, "s3Out")

	output := &s3Output{
		beatName:    "beatName",
		s3Svc:       nil,
		consumerMap: make(map[string]ConsumerAPI),
		consumerWg:  &sync.WaitGroup{},
	}

	config := defaultConfig
	config.TemporaryDirectory = outputTempDir

	err := output.init(config)
	assert.Nil(t, err)

	consumerMock1 := new(consumerMock)
	output.consumerMap["myApp1"] = consumerMock1
	consumerMock2 := new(consumerMock)
	output.consumerMap["myApp2"] = consumerMock2

	data := outputs.Data{
		Event: common.MapStr{},
	}
	data.Event["source"] = filepath.Join("var", "log", "myApp.log")
	data.Event["fields"] = common.MapStr{}
	data.Event["fields"].(common.MapStr)["appType"] = "myApp1"
	data.Event["message"] = "Hello myApp1!"

	consumerMock1.On("AppendLine", "Hello myApp1!").Return()
	consumerMock2.On("AppendLine", "Hello myApp2!").Return()

	err = output.PublishEvent(nil, outputs.Options{}, data)
	assert.Nil(t, err)
	consumerMock1.AssertNumberOfCalls(t, "AppendLine", 1)
	consumerMock2.AssertNotCalled(t, "AppendLine")

	data.Event["fields"].(common.MapStr)["appType"] = "myApp2"
	data.Event["message"] = "Hello myApp2!"

	err = output.PublishEvent(nil, outputs.Options{}, data)
	assert.Nil(t, err)
	consumerMock1.AssertNumberOfCalls(t, "AppendLine", 1)
	consumerMock2.AssertNumberOfCalls(t, "AppendLine", 1)
}

func TestClose(t *testing.T) {
	tempDir := mkTempDir(t)
	defer rmTempDir(t, tempDir)
	outputTempDir := filepath.Join(tempDir, "s3Out")

	output := &s3Output{
		beatName:    "beatName",
		s3Svc:       nil,
		consumerMap: make(map[string]ConsumerAPI),
		consumerWg:  &sync.WaitGroup{},
	}

	config := defaultConfig
	config.TemporaryDirectory = outputTempDir

	err := output.init(config)
	assert.Nil(t, err)

	consumerMock1 := new(consumerMock)
	output.consumerMap["myApp1"] = consumerMock1
	consumerMock2 := new(consumerMock)
	output.consumerMap["myApp2"] = consumerMock2

	consumerMock1.On("Shutdown").Return()
	consumerMock2.On("Shutdown").Return()

	err = output.Close()
	assert.Nil(t, err)
	consumerMock1.AssertNumberOfCalls(t, "Shutdown", 1)
	consumerMock2.AssertNumberOfCalls(t, "Shutdown", 1)
}

func TestTicker(t *testing.T) {
	tempDir := mkTempDir(t)
	defer rmTempDir(t, tempDir)
	outputTempDir := filepath.Join(tempDir, "s3Out")

	output := &s3Output{
		beatName:    "beatName",
		s3Svc:       nil,
		consumerMap: make(map[string]ConsumerAPI),
		consumerWg:  &sync.WaitGroup{},
	}

	config := defaultConfig
	config.TemporaryDirectory = outputTempDir
	config.SecondsPerChunk = 1

	err := output.init(config)
	assert.Nil(t, err)

	consumerMock1 := new(consumerMock)
	output.consumerMap["myApp1"] = consumerMock1
	consumerMock2 := new(consumerMock)
	output.consumerMap["myApp2"] = consumerMock2

	consumer1Ticked := make(chan bool)
	consumer2Ticked := make(chan bool)

	consumerMock1.On("Tick", mock.AnythingOfType("time.Time")).Return().Run(func(args mock.Arguments) {
		output.ticker.Stop()
		consumer1Ticked <- true
	})
	consumerMock2.On("Tick", mock.AnythingOfType("time.Time")).Return().Run(func(args mock.Arguments) {
		output.ticker.Stop()
		consumer2Ticked <- true
	})

	select {
	case <-consumer1Ticked:
	case <-time.After(time.Second * 5):
		t.Error("Timed out waiting for consumer2 to recieve a tick")
	}

	select {
	case <-consumer2Ticked:
	case <-time.After(time.Second * 5):
		t.Error("Timed2out waiting for consumer1 to recieve a tick")
	}

	consumerMock1.AssertExpectations(t)
	consumerMock2.AssertExpectations(t)
}
