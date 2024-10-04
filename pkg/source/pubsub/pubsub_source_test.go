/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package pubsubsource

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/hcl/v2/hclparse"
	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/assets"
	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/source/sourceconfig"
	"github.com/snowplow/snowbridge/pkg/testutil"
)

func TestMain(m *testing.M) {
	os.Clearenv()
	exitVal := m.Run()
	os.Exit(exitVal)
}

func TestPubSubSource_ReadAndReturnSuccessIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	assert := assert.New(t)

	// Create topic and subscription
	topic, subscription := testutil.CreatePubSubTopicAndSubscription(t, "test-topic", "test-sub")
	defer topic.Delete(context.Background())
	defer subscription.Delete(context.Background())
	// Write to topic
	testutil.WriteToPubSubTopic(t, topic, 10)

	filename := filepath.Join(assets.AssetsRootDir, "test", "config", "configs", "empty.hcl")
	t.Setenv("SNOWBRIDGE_CONFIG_FILE", filename)

	adaptedHandle := adapterGenerator(configFunction)

	pubsubSourceConfigPair := config.ConfigurationPair{Name: "pubsub", Handle: adaptedHandle}
	supportedSources := []config.ConfigurationPair{pubsubSourceConfigPair}

	// Construct the config
	pubsubConfig, err := config.NewConfig()
	assert.NotNil(pubsubConfig)
	if err != nil {
		t.Fatalf("unexpected error: %q", err.Error())
	}

	configBytesToMerge := []byte(`
    project_id      = "project-test"
    subscription_id = "test-sub"
`)

	parser := hclparse.NewParser()
	fileHCL, diags := parser.ParseHCL(configBytesToMerge, "placeholder")
	if diags.HasErrors() {
		t.Fatalf("failed to parse config bytes")
	}

	pubsubConfig.Data.Source.Use.Name = "pubsub"
	pubsubConfig.Data.Source.Use.Body = fileHCL.Body

	pubsubSource, err := sourceconfig.GetSource(pubsubConfig, supportedSources)

	assert.NotNil(pubsubSource)
	assert.Nil(err)
	assert.Equal("projects/project-test/subscriptions/test-sub", pubsubSource.GetID())

	output := testutil.ReadAndReturnMessages(pubsubSource, 5*time.Second, testutil.DefaultTestWriteBuilder, nil)
	assert.Equal(10, len(output))
	for _, message := range output {
		assert.Contains(string(message.Data), `message #`)
		assert.Nil(message.GetError())
	}
}

// newPubSubSource_Failure should fail if we can't reach PubSub, commented out this test until we look into https://github.com/snowplow/snowbridge/issues/151
/*
func TestNewPubSubSource_Failure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	assert := assert.New(t)

	pubsubSource, err := newPubSubSource(10, "nonexistent-project", "nonexistent-subscription")
	assert.NotNil(err)
	assert.Nil(pubsubSource)
	// This should return an error when we can't connect, rather than proceeding to the Write() function before we hit a problem.
}
*/

// TestNewPubSubSource_Success tests the typical case of creating a new pubsub source.
func TestNewPubSubSource_Success(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	assert := assert.New(t)

	testutil.InitMockPubsubServer(8010, nil, t)

	pubsubSource, err := newPubSubSource(10, "project-test", "test-sub", 1000, 1e9)
	assert.Nil(err)
	assert.IsType(&pubSubSource{}, pubsubSource)
	// This should return an error when we can't connect, rather than proceeding to the Write() function before we hit a problem.
}

func TestPubSubSource_ReadAndReturnSuccessWithMock(t *testing.T) {
	assert := assert.New(t)

	srv, conn := testutil.InitMockPubsubServer(8008, nil, t)
	defer srv.Close()
	defer conn.Close()

	// Publish ten messages
	numMsgs := 10
	wg := sync.WaitGroup{}
	for i := 0; i < numMsgs; i++ {
		wg.Add(1)
		go func(i int) {
			_ = srv.Publish(`projects/project-test/topics/test-topic`, []byte(strconv.Itoa(i)), nil)
			wg.Done()
		}(i)
	}
	wg.Wait()

	pubsubSource, err := newPubSubSource(10, "project-test", "test-sub", 1000, 1e9)

	assert.NotNil(pubsubSource)
	assert.Nil(err)
	assert.Equal("projects/project-test/subscriptions/test-sub", pubsubSource.GetID())

	output := testutil.ReadAndReturnMessages(pubsubSource, 3*time.Second, testutil.DefaultTestWriteBuilder, nil)
	assert.Equal(10, len(output))

	// Check that we got exactly the 10 messages we want, with no duplicates
	msgDatas := make([]string, 0)
	for _, msg := range output {
		msgDatas = append(msgDatas, string(msg.Data))
	}
	expected := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}
	sort.Strings(msgDatas)
	assert.Equal(expected, msgDatas)
}

// TestPubSubSource_ReadAndReturnSuccessWithMock_DelayedAcks tests the behaviour of pubsub source when some messages take longer to ack than others
func TestPubSubSource_ReadAndReturnSuccessWithMock_DelayedAcks(t *testing.T) {
	assert := assert.New(t)

	srv, conn := testutil.InitMockPubsubServer(8008, nil, t)
	defer srv.Close()
	defer conn.Close()

	// publish 10 messages
	numMsgs := 10
	wg := sync.WaitGroup{}
	for i := 0; i < numMsgs; i++ {
		wg.Add(1)
		go func(i int) {
			_ = srv.Publish(`projects/project-test/topics/test-topic`, []byte(strconv.Itoa(i)), nil)
			wg.Done()
		}(i)
	}
	wg.Wait()

	filename := filepath.Join(assets.AssetsRootDir, "test", "config", "configs", "empty.hcl")
	t.Setenv("SNOWBRIDGE_CONFIG_FILE", filename)

	adaptedHandle := adapterGenerator(configFunction)

	pubsubSourceConfigPair := config.ConfigurationPair{Name: "pubsub", Handle: adaptedHandle}
	supportedSources := []config.ConfigurationPair{pubsubSourceConfigPair}

	// Construct the config
	pubsubConfig, err := config.NewConfig()
	assert.NotNil(pubsubConfig)
	if err != nil {
		t.Fatalf("unexpected error: %q", err.Error())
	}

	configBytesToMerge := []byte(`
    project_id      = "project-test"
    subscription_id = "test-sub"
`)

	parser := hclparse.NewParser()
	fileHCL, diags := parser.ParseHCL(configBytesToMerge, "placeholder")
	if diags.HasErrors() {
		t.Fatalf("failed to parse config bytes")
	}

	pubsubConfig.Data.Source.Use.Name = "pubsub"
	pubsubConfig.Data.Source.Use.Body = fileHCL.Body

	pubsubSource, err := sourceconfig.GetSource(pubsubConfig, supportedSources)

	assert.NotNil(pubsubSource)
	assert.Nil(err)
	assert.Equal("projects/project-test/subscriptions/test-sub", pubsubSource.GetID())

	output := testutil.ReadAndReturnMessages(pubsubSource, 5*time.Second, testutil.DelayedAckTestWriteBuilder, 2*time.Second)
	assert.Equal(10, len(output))

	// Check that we got exactly the 10 messages we want, with no duplicates
	msgDatas := make([]string, 0)
	for _, msg := range output {
		msgDatas = append(msgDatas, string(msg.Data))
	}
	expected := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}
	sort.Strings(msgDatas)
	assert.Equal(expected, msgDatas)
}
