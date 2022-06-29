// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package pubsubsource

import (
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/snowplow-devops/stream-replicator/config"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceconfig"
	"github.com/snowplow-devops/stream-replicator/pkg/testutil"
)

func TestPubSubSource_ReadAndReturnSuccessIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	assert := assert.New(t)

	// Create pubsub integration resource and populate with 10 messages
	testutil.CreatePubsubResourcesAndWrite(10)
	defer testutil.DeletePubsubResources()

	t.Setenv("SOURCE_NAME", "pubsub")
	t.Setenv("SOURCE_PUBSUB_SUBSCRIPTION_ID", "test-sub")
	t.Setenv("SOURCE_PUBSUB_PROJECT_ID", `project-test`)

	adaptedHandle := adapterGenerator(configFunction)

	pubsubSourceConfigPair := sourceconfig.ConfigPair{Name: "pubsub", Handle: adaptedHandle}
	supportedSources := []sourceconfig.ConfigPair{pubsubSourceConfigPair}

	pubsubConfig, err := config.NewConfig()
	assert.NotNil(pubsubConfig)
	assert.Nil(err)

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

// GetSource should fail if we can't reach PubSub, commented out this test until we look into https://github.com/snowplow-devops/stream-replicator/issues/151
/*
func TestGetSource_Failure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	assert := assert.New(t)

	t.Setenv("SOURCE_NAME", "pubsub")
	t.Setenv("SOURCE_PUBSUB_SUBSCRIPTION_ID", "not-exists")
	t.Setenv("SOURCE_PUBSUB_PROJECT_ID",  `project-test`)

	adaptedHandle := adapterGenerator(configFunction)

	pubsubSourceConfigPair := sourceconfig.ConfigPair{Name: "pubsub", Handle: adaptedHandle}
	supportedSources := []sourceconfig.ConfigPair{pubsubSourceConfigPair}

	pubsubConfig, err := config.NewConfig()
	assert.NotNil(pubsubConfig)
	assert.Nil(err)

	pubsubSource, err := sourceconfig.GetSource(pubsubConfig, supportedSources)
	assert.NotNil(err)
	// This should return an error when we can't connect, rather than proceeding to the Write() function before we hit a problem.
}
*/

func TestPubSubSource_ReadAndReturnSuccessWithMock(t *testing.T) {
	assert := assert.New(t)

	srv, conn := testutil.InitMockPubsubServer(8008, nil)
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

	t.Setenv("SOURCE_NAME", "pubsub")
	t.Setenv("SOURCE_PUBSUB_SUBSCRIPTION_ID", "test-sub")
	t.Setenv("SOURCE_PUBSUB_PROJECT_ID", `project-test`)

	adaptedHandle := adapterGenerator(configFunction)

	pubsubSourceConfigPair := sourceconfig.ConfigPair{Name: "pubsub", Handle: adaptedHandle}
	supportedSources := []sourceconfig.ConfigPair{pubsubSourceConfigPair}

	pubsubConfig, err := config.NewConfig()
	assert.NotNil(pubsubConfig)
	assert.Nil(err)

	pubsubSource, err := sourceconfig.GetSource(pubsubConfig, supportedSources)

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

	srv, conn := testutil.InitMockPubsubServer(8008, nil)
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

	t.Setenv("SOURCE_NAME", "pubsub")
	t.Setenv("SOURCE_PUBSUB_SUBSCRIPTION_ID", "test-sub")
	t.Setenv("SOURCE_PUBSUB_PROJECT_ID", `project-test`)

	adaptedHandle := adapterGenerator(configFunction)

	pubsubSourceConfigPair := sourceconfig.ConfigPair{Name: "pubsub", Handle: adaptedHandle}
	supportedSources := []sourceconfig.ConfigPair{pubsubSourceConfigPair}

	pubsubConfig, err := config.NewConfig()
	assert.NotNil(pubsubConfig)
	assert.Nil(err)

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
