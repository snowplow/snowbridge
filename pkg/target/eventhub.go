// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package target

import (
	"context"
	"fmt"
	"os"
	"time"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
)

// EventHubConfig holds a config object for Azure EventHub
type EventHubConfig struct {
	EventHubNamespace       string `hcl:"namespace" env:"TARGET_EVENTHUB_NAMESPACE"`
	EventHubName            string `hcl:"name" env:"TARGET_EVENTHUB_NAME"`
	MaxAutoRetries          int    `hcl:"max_auto_retries,optional" env:"TARGET_EVENTHUB_MAX_AUTO_RETRY"`
	MessageByteLimit        int    `hcl:"message_byte_limit,optional" env:"TARGET_EVENTHUB_MESSAGE_BYTE_LIMIT"`
	ChunkByteLimit          int    `hcl:"chunk_byte_limit,optional" env:"TARGET_EVENTHUB_CHUNK_BYTE_LIMIT"`
	ChunkMessageLimit       int    `hcl:"chunk_message_limit,optional" env:"TARGET_EVENTHUB_CHUNK_MESSAGE_LIMIT"`
	ContextTimeoutInSeconds int    `hcl:"context_timeout_in_seconds,optional" env:"TARGET_EVENTHUB_CONTEXT_TIMEOUT_SECONDS"`
	BatchByteLimit          int    `hcl:"batch_byte_limit,optional" env:"TARGET_EVENTHUB_BATCH_BYTE_LIMIT"`
	SetEHPartitionKey       bool   `hcl:"set_eh_partition_key,optional" env:"TARGET_EVENTHUB_SET_EH_PK"`
}

// EventHubTarget holds a new client for writing messages to Azure EventHub
type EventHubTarget struct {
	client                  clientIface
	eventHubNamespace       string
	eventHubName            string
	messageByteLimit        int
	chunkByteLimit          int
	chunkMessageLimit       int
	contextTimeoutInSeconds int
	batchByteLimit          int
	setEHPartitionKey       bool

	log *log.Entry
}

// clientIface allows us to mock the entire eventhub.Hub client, since they don't provide interfaces for mocking https://github.com/Azure/azure-event-hubs-go/issues/98
type clientIface interface {
	SendBatch(context.Context, eventhub.BatchIterator, ...eventhub.BatchOption) error
	Close(context.Context) error
}

// newEventHubTargetWithInterfaces allows for mocking the eventhub client
func newEventHubTargetWithInterfaces(client clientIface, cfg *EventHubConfig) *EventHubTarget {
	return &EventHubTarget{
		client:                  client,
		eventHubNamespace:       cfg.EventHubNamespace,
		eventHubName:            cfg.EventHubName,
		messageByteLimit:        cfg.MessageByteLimit,
		chunkByteLimit:          cfg.ChunkByteLimit,
		chunkMessageLimit:       cfg.ChunkMessageLimit,
		contextTimeoutInSeconds: cfg.ContextTimeoutInSeconds,
		batchByteLimit:          cfg.BatchByteLimit,
		setEHPartitionKey:       cfg.SetEHPartitionKey,

		log: log.WithFields(log.Fields{"target": "eventhub", "cloud": "Azure", "namespace": cfg.EventHubNamespace, "eventhub": cfg.EventHubName}),
	}
}

// newEventHubTarget creates a new client for writing messages to Azure EventHub
func newEventHubTarget(cfg *EventHubConfig) (*EventHubTarget, error) {

	_, keyNamePresent := os.LookupEnv("EVENTHUB_KEY_NAME")
	_, keyValuePresent := os.LookupEnv("EVENTHUB_KEY_VALUE")

	_, connStringPresent := os.LookupEnv("EVENTHUB_CONNECTION_STRING")

	_, tenantIDPresent := os.LookupEnv("AZURE_TENANT_ID")
	_, clientIDPresent := os.LookupEnv("AZURE_CLIENT_ID")

	_, clientSecretPresent := os.LookupEnv("AZURE_CLIENT_SECRET")

	_, azCertPathPresent := os.LookupEnv("AZURE_CERTIFICATE_PATH")
	_, azCertPwrdPresent := os.LookupEnv("AZURE_CERTIFICATE_PASSWORD")

	if !(connStringPresent || (keyNamePresent && keyValuePresent) || (tenantIDPresent && clientIDPresent && ((azCertPathPresent && azCertPwrdPresent) || clientSecretPresent))) {
		return nil, errors.Errorf("Error initialising EventHub client: No valid combination of authentication Env vars found. https://pkg.go.dev/github.com/Azure/azure-event-hubs-go#NewHubWithNamespaceNameAndEnvironment")
	}

	hub, err := eventhub.NewHubWithNamespaceNameAndEnvironment(cfg.EventHubNamespace, cfg.EventHubName, eventhub.HubWithSenderMaxRetryCount(cfg.MaxAutoRetries))
	// Using HubWithSenderMaxRetryCount limits the amount of retries that are handled by the eventhubs package natively (this app handles retries externally to this also)
	// If none is specified, it will retry indefinitely until the context times out, which hides the actual error message
	// To avoid obscuring errors, contextTimeoutInSeconds should be configured to ensure all retries may be completed before its expiry

	// get the runtime information of the event hub in order to check the connection
	_, err = hub.GetRuntimeInformation(context.Background())
	if err != nil {
		return nil, errors.Errorf("Error initialising EventHub client: could not reach Event Hub: %v", err)
	}

	return newEventHubTargetWithInterfaces(hub, cfg), err
}

// EventHubTargetConfigFunction creates an EventHubTarget from an EventHubconfig
func EventHubTargetConfigFunction(cfg *EventHubConfig) (*EventHubTarget, error) {
	return newEventHubTarget(cfg)
}

// The EventHubTargetAdapter type is an adapter for functions to be used as
// pluggable components for EventHub target. Implements the Pluggable interface.
type EventHubTargetAdapter func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface.
func (f EventHubTargetAdapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface.
func (f EventHubTargetAdapter) ProvideDefault() (interface{}, error) {
	// Provide defaults for the optional parameters
	// whose default is not their zero value.
	cfg := &EventHubConfig{
		MaxAutoRetries:          1,
		MessageByteLimit:        1048576,
		ChunkByteLimit:          1048576,
		ChunkMessageLimit:       500,
		ContextTimeoutInSeconds: 20,
		BatchByteLimit:          1048576,
		SetEHPartitionKey:       true,
	}

	return cfg, nil
}

// AdaptEventHubTargetFunc returns an EventHubTargetAdapter.
func AdaptEventHubTargetFunc(f func(c *EventHubConfig) (*EventHubTarget, error)) EventHubTargetAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*EventHubConfig)
		if !ok {
			return nil, errors.New("invalid input, expected EventHubConfig")
		}

		return f(cfg)
	}
}

func (eht *EventHubTarget) Write(messages []*models.Message) (*models.TargetWriteResult, error) {
	eht.log.Debugf("Writing %d messages to stream ...", len(messages))

	chunks, oversized := models.GetChunkedMessages(
		messages,
		eht.chunkMessageLimit,                // Max Chunk size (number of messages)
		eht.MaximumAllowedMessageSizeBytes(), // Message byte limit
		eht.chunkByteLimit,                   // Chunk byte limit
	)

	writeResult := &models.TargetWriteResult{
		Oversized: oversized,
	}

	var errResult error

	for _, chunk := range chunks {
		res, err := eht.process(chunk)
		writeResult = writeResult.Append(res)

		if err != nil {
			errResult = multierror.Append(errResult, err)
		}
	}

	if errResult != nil {
		errResult = errors.Wrap(errResult, "Error writing messages to EventHub")
	}

	eht.log.Debugf("Successfully wrote %d/%d messages", writeResult.SentCount, writeResult.Total())
	return writeResult, errResult
}

func (eht *EventHubTarget) process(messages []*models.Message) (*models.TargetWriteResult, error) {
	messageCount := len(messages)
	eht.log.Debugf("Writing chunk of %d messages to eventHub ...", messageCount)

	ehBatch := make([]*eventhub.Event, messageCount)
	for i, msg := range messages {
		ehEvent := eventhub.NewEvent(msg.Data)
		if eht.setEHPartitionKey {
			ehEvent.PartitionKey = &msg.PartitionKey
		}
		ehBatch[i] = ehEvent
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(eht.contextTimeoutInSeconds)*time.Second)
	defer cancel()

	batchIterator := eventhub.NewEventBatchIterator(ehBatch...)
	err := eht.client.SendBatch(ctx, batchIterator, eventhub.BatchWithMaxSizeInBytes(eht.batchByteLimit))

	if err != nil {
		// If we hit an error, we can't distinguish successful batches from the failed one(s), so we return the whole chunk as failed
		return models.NewTargetWriteResult(
			nil,
			messages,
			nil,
			nil,
		), errors.Wrap(err, "Failed to send message batch to EventHub")
	}

	// If no error, all messages were successes
	for _, msg := range messages {
		if msg.AckFunc != nil {
			msg.AckFunc()
		}
	}

	eht.log.Debugf("Successfully wrote chunk of %d messages", len(messages))
	return models.NewTargetWriteResult(
		messages,
		nil,
		nil,
		nil,
	), nil
}

// Open does not do anything for this target
func (eht *EventHubTarget) Open() {}

// Close closes the eventhub client.
func (eht *EventHubTarget) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(10)*time.Second)
	defer cancel()
	eht.client.Close(ctx)
}

// MaximumAllowedMessageSizeBytes returns the max number of bytes that can be sent
// per message for this target
func (eht *EventHubTarget) MaximumAllowedMessageSizeBytes() int {
	return eht.messageByteLimit
}

// GetID returns an identifier for this target
func (eht *EventHubTarget) GetID() string {
	return fmt.Sprintf("sb://%s.servicebus.windows.net/;EntityPath=%s", eht.eventHubNamespace, eht.eventHubName)
}
