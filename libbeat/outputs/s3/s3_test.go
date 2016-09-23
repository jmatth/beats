// +build !integration

package s3out

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/outputs"
	"github.com/stretchr/testify/assert"
)

func TestOutputInit(t *testing.T) {
	tempDir := mkTempDir(t)
	defer rmTempDir(t, tempDir)
	outputTempDir := filepath.Join(tempDir, "s3Out")

	output := &s3Output{
		s3Svc:        nil,
		consumerLock: new(sync.RWMutex),
		consumerMap:  make(map[string]consumerAPI),
		consumerWg:   &sync.WaitGroup{},
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
		s3Svc:        nil,
		consumerLock: new(sync.RWMutex),
		consumerMap:  make(map[string]consumerAPI),
		consumerWg:   &sync.WaitGroup{},
	}

	config := defaultConfig
	config.TemporaryDirectory = outputTempDir

	err := output.init(config)
	assert.Nil(t, err)

	options := &consumerOptions{AppType: "myApp"}
	myConsumerMock := new(consumerMock)
	output.consumerMap[options.AppType] = myConsumerMock
	outConsumer, err := output.getConsumer(options)
	assert.Nil(t, err)
	assert.Equal(t, outConsumer, myConsumerMock)

	otherOptions := &consumerOptions{AppType: "otherApp"}
	realConsumer, err := output.getConsumer(otherOptions)
	realConsumer.shutdown()
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

func TestGetConsumerOptions(t *testing.T) {
	data := outputs.Data{
		Event: common.MapStr{},
	}

	ret, err := getConsumerOptions(data)
	assert.NotNil(t, err)

	data.Event["source"] = filepath.Join("var", "log", "myApp.log")
	ret, err = getConsumerOptions(data)
	assert.Nil(t, err)
	assert.Equal(t, "myApp.log", ret.AppType)
	assert.Empty(t, ret.TimestampFormat)
	assert.Empty(t, ret.TimestampRegex)

	data.Event["fields"] = common.MapStr{}
	data.Event["fields"].(common.MapStr)["s3"] = common.MapStr{}
	data.Event["fields"].(common.MapStr)["s3"].(common.MapStr)["appType"] = "myApp"

	ret, err = getConsumerOptions(data)
	assert.Nil(t, err)
	assert.Equal(t, "myApp", ret.AppType)
	assert.Empty(t, ret.TimestampFormat)
	assert.Empty(t, ret.TimestampRegex)

	data.Event["fields"].(common.MapStr)["s3"].(common.MapStr)["timestampRegex"] = "someRegex"
	data.Event["fields"].(common.MapStr)["s3"].(common.MapStr)["timestampFormat"] = "someFormat"
	ret, err = getConsumerOptions(data)
	assert.Nil(t, err)
	assert.Equal(t, "myApp", ret.AppType)
	assert.Equal(t, "someFormat", ret.TimestampFormat)
	assert.Equal(t, "someRegex", ret.TimestampRegex)
}

func TestPublishEvent(t *testing.T) {
	tempDir := mkTempDir(t)
	defer rmTempDir(t, tempDir)
	outputTempDir := filepath.Join(tempDir, "s3Out")

	output := &s3Output{
		s3Svc:        nil,
		consumerLock: new(sync.RWMutex),
		consumerMap:  make(map[string]consumerAPI),
		consumerWg:   &sync.WaitGroup{},
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
	data.Event["fields"].(common.MapStr)["s3"] = common.MapStr{}
	data.Event["fields"].(common.MapStr)["s3"].(common.MapStr)["appType"] = "myApp1"
	data.Event["message"] = "Hello myApp1!"

	consumerMock1.On("appendLine", "Hello myApp1!").Return()
	consumerMock2.On("appendLine", "Hello myApp2!").Return()

	err = output.PublishEvent(nil, outputs.Options{}, data)
	assert.Nil(t, err)
	consumerMock1.AssertNumberOfCalls(t, "appendLine", 1)
	consumerMock2.AssertNotCalled(t, "appendLine")

	data.Event["fields"].(common.MapStr)["s3"] = common.MapStr{}
	data.Event["fields"].(common.MapStr)["s3"].(common.MapStr)["appType"] = "myApp2"
	data.Event["message"] = "Hello myApp2!"

	err = output.PublishEvent(nil, outputs.Options{}, data)
	assert.Nil(t, err)
	consumerMock1.AssertNumberOfCalls(t, "appendLine", 1)
	consumerMock2.AssertNumberOfCalls(t, "appendLine", 1)
}

func TestClose(t *testing.T) {
	tempDir := mkTempDir(t)
	defer rmTempDir(t, tempDir)
	outputTempDir := filepath.Join(tempDir, "s3Out")

	output := &s3Output{
		s3Svc:        nil,
		consumerLock: new(sync.RWMutex),
		consumerMap:  make(map[string]consumerAPI),
		consumerWg:   &sync.WaitGroup{},
	}

	config := defaultConfig
	config.TemporaryDirectory = outputTempDir

	err := output.init(config)
	assert.Nil(t, err)

	consumerMock1 := new(consumerMock)
	output.consumerMap["myApp1"] = consumerMock1
	consumerMock2 := new(consumerMock)
	output.consumerMap["myApp2"] = consumerMock2

	consumerMock1.On("shutdown").Return()
	consumerMock2.On("shutdown").Return()

	err = output.Close()
	assert.Nil(t, err)
	consumerMock1.AssertNumberOfCalls(t, "shutdown", 1)
	consumerMock2.AssertNumberOfCalls(t, "shutdown", 1)
}
