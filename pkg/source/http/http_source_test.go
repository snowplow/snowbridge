/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package httpsource

import (
	"bytes"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/v3/assets"
	config "github.com/snowplow/snowbridge/v3/config"
	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/source/sourceconfig"
	"github.com/snowplow/snowbridge/v3/pkg/source/sourceiface"
	"github.com/snowplow/snowbridge/v3/pkg/testutil"
)

func TestMain(m *testing.M) {
	os.Clearenv()
	exitVal := m.Run()
	os.Exit(exitVal)
}

func TestHTTPSource_NewHTTPSource(t *testing.T) {
	assert := assert.New(t)

	source, err := newHTTPSource(8080)
	assert.NotNil(source)
	assert.Nil(err)
	assert.Equal("http", source.GetID())
	assert.Equal(8080, source.port)
}

func TestHTTPSource_SingleLinePost(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	source, err := newHTTPSource(9090)
	assert.NotNil(source)
	assert.Nil(err)

	// Start the source in a goroutine
	var receivedMessages []*models.Message
	writeFunc := func(messages []*models.Message) error {
		receivedMessages = append(receivedMessages, messages...)
		for _, msg := range messages {
			msg.AckFunc()
		}
		return nil
	}

	sf := sourceiface.SourceFunctions{
		WriteToTarget: writeFunc,
	}

	go func() {
		err := source.Read(&sf)
		if err != nil {
			logrus.Error(err)
		}
	}()

	// Give the server time to start
	time.Sleep(100 * time.Millisecond)

	// Send a POST request
	testData := "test-line-1"
	resp, err := http.Post("http://localhost:9090", "text/plain", bytes.NewReader([]byte(testData)))
	assert.Nil(err)
	assert.Equal(http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	// Give time for message processing
	time.Sleep(100 * time.Millisecond)

	source.Stop()

	// Verify we received the message
	assert.Equal(1, len(receivedMessages))
	assert.Equal("test-line-1", string(receivedMessages[0].Data))
}

func TestHTTPSource_MultilinePost(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	source, err := newHTTPSource(9091)
	assert.NotNil(source)
	assert.Nil(err)

	// Start the source in a goroutine
	var receivedMessages []*models.Message
	writeFunc := func(messages []*models.Message) error {
		receivedMessages = append(receivedMessages, messages...)
		for _, msg := range messages {
			msg.AckFunc()
		}
		return nil
	}

	sf := sourceiface.SourceFunctions{
		WriteToTarget: writeFunc,
	}

	go func() {
		err := source.Read(&sf)
		if err != nil {
			logrus.Error(err)
		}
	}()

	// Give the server time to start
	time.Sleep(100 * time.Millisecond)

	// Send a POST request with multiple lines
	testData := "test-line-1\ntest-line-2\ntest-line-3"
	resp, err := http.Post("http://localhost:9091", "text/plain", bytes.NewReader([]byte(testData)))
	assert.Nil(err)
	assert.Equal(http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	// Give time for message processing
	time.Sleep(100 * time.Millisecond)

	source.Stop()

	// Verify we received all messages
	assert.Equal(3, len(receivedMessages))
	assert.Equal("test-line-1", string(receivedMessages[0].Data))
	assert.Equal("test-line-2", string(receivedMessages[1].Data))
	assert.Equal("test-line-3", string(receivedMessages[2].Data))
}

func TestHTTPSource_EmptyLinesIgnored(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	source, err := newHTTPSource(9092)
	assert.NotNil(source)
	assert.Nil(err)

	// Start the source in a goroutine
	var receivedMessages []*models.Message
	writeFunc := func(messages []*models.Message) error {
		receivedMessages = append(receivedMessages, messages...)
		for _, msg := range messages {
			msg.AckFunc()
		}
		return nil
	}

	sf := sourceiface.SourceFunctions{
		WriteToTarget: writeFunc,
	}

	go func() {
		err := source.Read(&sf)
		if err != nil {
			logrus.Error(err)
		}
	}()

	// Give the server time to start
	time.Sleep(100 * time.Millisecond)

	// Send a POST request with empty lines that should be ignored
	testData := "test-line-1\n\ntest-line-2\n"
	resp, err := http.Post("http://localhost:9092", "text/plain", bytes.NewReader([]byte(testData)))
	assert.Nil(err)
	assert.Equal(http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	// Give time for message processing
	time.Sleep(100 * time.Millisecond)

	source.Stop()

	// Verify we received only non-empty messages
	assert.Equal(2, len(receivedMessages))
	assert.Equal("test-line-1", string(receivedMessages[0].Data))
	assert.Equal("test-line-2", string(receivedMessages[1].Data))
}

func TestHTTPSource_MethodNotAllowed(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	source, err := newHTTPSource(9093)
	assert.NotNil(source)
	assert.Nil(err)

	// Start the source in a goroutine
	writeFunc := func(messages []*models.Message) error {
		return nil
	}

	sf := sourceiface.SourceFunctions{
		WriteToTarget: writeFunc,
	}

	go func() {
		err := source.Read(&sf)
		if err != nil {
			logrus.Error(err)
		}
	}()

	// Give the server time to start
	time.Sleep(100 * time.Millisecond)

	// Send a GET request (should fail)
	resp, err := http.Get("http://localhost:9093")
	assert.Nil(err)
	assert.Equal(http.StatusMethodNotAllowed, resp.StatusCode)
	resp.Body.Close()

	source.Stop()
}

func TestHTTPSource_WithTestUtil(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	source, err := newHTTPSource(9094)
	assert.NotNil(source)
	assert.Nil(err)

	// Start the source and send data using testutil pattern
	go func() {
		// Give the server time to start
		time.Sleep(100 * time.Millisecond)

		// Send test data
		testData := "testutil-line-1\ntestutil-line-2"
		resp, err := http.Post("http://localhost:9094", "text/plain", bytes.NewReader([]byte(testData)))
		if err != nil {
			logrus.Error(err)
			return
		}
		resp.Body.Close()
	}()

	// Use testutil to read and return messages
	output := testutil.ReadAndReturnMessages(source, 2*time.Second, testutil.DefaultTestWriteBuilder, nil)

	assert.Equal(2, len(output))
	assert.Equal("testutil-line-1", string(output[0].Data))
	assert.Equal("testutil-line-2", string(output[1].Data))

	// Verify no errors in the messages
	for _, message := range output {
		assert.Nil(message.GetError())
		assert.NotEmpty(message.PartitionKey)
		assert.False(message.TimeCreated.IsZero())
		assert.False(message.TimePulled.IsZero())
	}
}

func TestGetSource_WithHTTPSource(t *testing.T) {
	filename := filepath.Join(assets.AssetsRootDir, "test", "config", "configs", "empty.hcl")
	t.Setenv("SNOWBRIDGE_CONFIG_FILE", filename)

	assert := assert.New(t)

	supportedSources := []config.ConfigurationPair{ConfigPair}

	c, err := config.NewConfig()
	assert.NotNil(c)
	if err != nil {
		t.Fatalf("function NewConfig failed with error: %q", err.Error())
	}

	// Override the source configuration to use HTTP
	c.Data.Source.Use.Name = "http"

	httpSource, err := sourceconfig.GetSource(c, supportedSources)

	assert.NotNil(httpSource)
	assert.Nil(err)
	assert.Equal("http", httpSource.GetID())
}

func TestHTTPSource_DefaultConfiguration(t *testing.T) {
	assert := assert.New(t)

	adapter := adapterGenerator(configfunction)
	defaultConfig, err := adapter.ProvideDefault()
	assert.Nil(err)
	assert.NotNil(defaultConfig)

	config, ok := defaultConfig.(*Configuration)
	assert.True(ok)
	assert.Equal(8080, config.Port)
}

func TestHTTPSource_ConfigurationValidation(t *testing.T) {
	assert := assert.New(t)

	adapter := adapterGenerator(configfunction)

	// Test with valid configuration
	validConfig := &Configuration{Port: 9999}
	source, err := adapter.Create(validConfig)
	assert.Nil(err)
	assert.NotNil(source)

	httpSource, ok := source.(sourceiface.Source)
	assert.True(ok)
	assert.Equal("http", httpSource.GetID())

	// Test with invalid configuration type
	invalidConfig := "not a config"
	source, err = adapter.Create(invalidConfig)
	assert.NotNil(err)
	assert.Nil(source)
	assert.Contains(err.Error(), "invalid input")
}