// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package target

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var cfg = EventHubConfig{
	EventHubNamespace: "test",
	EventHubName:      "test",
}

func unsetEverything() {
	os.Unsetenv("EVENTHUB_KEY_NAME")
	os.Unsetenv("EVENTHUB_KEY_VALUE")

	os.Unsetenv("EVENTHUB_CONNECTION_STRING")

	os.Unsetenv("AZURE_TENANT_ID")
	os.Unsetenv("AZURE_CLIENT_ID")

	os.Unsetenv("AZURE_CLIENT_SECRET")

	os.Unsetenv("AZURE_CERTIFICATE_PATH")
	os.Unsetenv("AZURE_CERTIFICATE_PASSWORD")
}

func TestNewEventHubTarget_KeyValue(t *testing.T) {
	assert := assert.New(t)

	unsetEverything()

	// Test that we can initialise a client with Key and Value
	defer os.Unsetenv("EVENTHUB_KEY_NAME")
	defer os.Unsetenv("EVENTHUB_KEY_VALUE")

	os.Setenv("EVENTHUB_KEY_NAME", "fake")
	os.Setenv("EVENTHUB_KEY_VALUE", "fake")

	tgt, err := NewEventHubTarget(&cfg)
	assert.Nil(err)
	assert.NotNil(tgt)
}

func TestNewEventHubTarget_ConnString(t *testing.T) {
	assert := assert.New(t)

	unsetEverything()

	// Test that we can initialise a client with Connection String
	defer os.Unsetenv("EVENTHUB_CONNECTION_STRING")

	os.Setenv("EVENTHUB_CONNECTION_STRING", "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=fake;SharedAccessKey=fake")

	tgt, err := NewEventHubTarget(&cfg)
	assert.Nil(err)
	assert.NotNil(tgt)
}

func TestNewEventHubTarget_Failure(t *testing.T) {
	assert := assert.New(t)

	unsetEverything()

	tgt, err := NewEventHubTarget(&cfg)
	assert.Equal("Error initialising EventHub client: No valid combination of authentication Env vars found. https://pkg.go.dev/github.com/Azure/azure-event-hubs-go#NewHubWithNamespaceNameAndEnvironment", err.Error())
	assert.Nil(tgt)
}
