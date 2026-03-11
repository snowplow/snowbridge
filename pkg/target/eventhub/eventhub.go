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

package eventhub

import (
	"context"
	"fmt"
	"os"
	"time"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/Azure/go-amqp"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/target/targetiface"
)

const SupportedTargetEventHub = "eventhub"

// EventHubConfig holds a config object for Azure EventHub
type EventHubConfig struct {
	BatchingConfig          *targetiface.BatchingConfig `hcl:"batching,block"`
	EventHubNamespace       string                      `hcl:"namespace"`
	EventHubName            string                      `hcl:"name"`
	MaxAutoRetries          int                         `hcl:"max_auto_retries,optional"`
	ContextTimeoutInSeconds int                         `hcl:"context_timeout_in_seconds,optional"`
	SetEHPartitionKey       bool                        `hcl:"set_eh_partition_key,optional"`
}

// EventHubTargetDriver holds a new client for writing messages to Azure EventHub
type EventHubTargetDriver struct {
	BatchingConfig          targetiface.BatchingConfig
	client                  clientIface
	eventHubNamespace       string
	eventHubName            string
	contextTimeoutInSeconds int
	setEHPartitionKey       bool
	maxAutoRetries          int

	log *log.Entry
}

// clientIface allows us to mock the entire eventhub.Hub client, since they don't provide interfaces for mocking https://github.com/Azure/azure-event-hubs-go/issues/98
type clientIface interface {
	SendBatch(context.Context, eventhub.BatchIterator, ...eventhub.BatchOption) error
	Close(context.Context) error
}

// GetDefaultConfiguration returns the default configuration for EventHub target
func (eht *EventHubTargetDriver) GetDefaultConfiguration() any {
	return &EventHubConfig{
		BatchingConfig: &targetiface.BatchingConfig{
			MaxBatchMessages:     500,
			MaxBatchBytes:        1048576,
			MaxMessageBytes:      1048576,
			MaxConcurrentBatches: 5,
			FlushPeriodMillis:    500,
		},
		MaxAutoRetries:          1,
		ContextTimeoutInSeconds: 20,
		SetEHPartitionKey:       true,
	}
}

func (eht *EventHubTargetDriver) SetBatchingConfig(batchingConfig targetiface.BatchingConfig) {
	eht.BatchingConfig = batchingConfig
}

func (eht *EventHubTargetDriver) GetBatchingConfig() targetiface.BatchingConfig {
	return eht.BatchingConfig
}

// InitFromConfig creates a new client for writing messages to EventHub
func (eht *EventHubTargetDriver) InitFromConfig(c any) error {
	cfg, ok := c.(*EventHubConfig)
	if !ok {
		return fmt.Errorf("invalid configuration type")
	}

	_, keyNamePresent := os.LookupEnv("EVENTHUB_KEY_NAME")
	_, keyValuePresent := os.LookupEnv("EVENTHUB_KEY_VALUE")

	_, connStringPresent := os.LookupEnv("EVENTHUB_CONNECTION_STRING")

	_, tenantIDPresent := os.LookupEnv("AZURE_TENANT_ID")
	_, clientIDPresent := os.LookupEnv("AZURE_CLIENT_ID")

	_, clientSecretPresent := os.LookupEnv("AZURE_CLIENT_SECRET")

	_, azCertPathPresent := os.LookupEnv("AZURE_CERTIFICATE_PATH")
	_, azCertPwrdPresent := os.LookupEnv("AZURE_CERTIFICATE_PASSWORD")

	if !connStringPresent &&
		(!keyNamePresent || !keyValuePresent) ||
		(tenantIDPresent && clientIDPresent && ((azCertPathPresent && azCertPwrdPresent) || clientSecretPresent)) {
		return errors.Errorf("Error initialising EventHub client: No valid combination of authentication Env vars found. https://pkg.go.dev/github.com/Azure/azure-event-hubs-go#NewHubWithNamespaceNameAndEnvironment")
	}

	client, err := eventhub.NewHubWithNamespaceNameAndEnvironment(cfg.EventHubNamespace, cfg.EventHubName, eventhub.HubWithSenderMaxRetryCount(cfg.MaxAutoRetries))
	if err != nil {
		return err
	}

	return eht.newEventHubTargetDriverWithInterfaces(client, cfg)
}

// newEventHubTargetDriverWithInterfaces allows for mocking the eventhub client
func (eht *EventHubTargetDriver) newEventHubTargetDriverWithInterfaces(client clientIface, cfg *EventHubConfig) error {
	// Set the batching config - used in both the below and the batcher.
	eht.SetBatchingConfig(*cfg.BatchingConfig)

	eht.client = client
	eht.eventHubNamespace = cfg.EventHubNamespace
	eht.eventHubName = cfg.EventHubName
	eht.contextTimeoutInSeconds = cfg.ContextTimeoutInSeconds
	eht.setEHPartitionKey = cfg.SetEHPartitionKey
	eht.maxAutoRetries = cfg.MaxAutoRetries
	eht.log = log.WithFields(log.Fields{"target": SupportedTargetEventHub, "cloud": "Azure", "namespace": cfg.EventHubNamespace, "eventhub": cfg.EventHubName})

	return nil
}

// Batcher combines new data with current batch and returns batches ready to send, new current batch, and oversized messages
func (eht *EventHubTargetDriver) Batcher(currentBatch targetiface.CurrentBatch, message *models.Message) (batchToSend []*models.Message, newCurrentBatch targetiface.CurrentBatch, oversized *models.Message) {
	return targetiface.DefaultBatcher(currentBatch, message, eht.BatchingConfig)
}

// Write pushes all messages to the required target
func (eht *EventHubTargetDriver) Write(messages []*models.Message) (*models.TargetWriteResult, error) {
	eht.log.Debugf("Writing %d messages to EventHub ...", len(messages))

	ehBatch := make([]*eventhub.Event, len(messages))
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
	requestStarted := time.Now().UTC()
	err := eht.client.SendBatch(ctx, batchIterator, eventhub.BatchWithMaxSizeInBytes(eht.BatchingConfig.MaxBatchBytes))
	requestFinished := time.Now().UTC()

	// Assign timings to all messages
	for _, msg := range messages {
		msg.TimeRequestStarted = requestStarted
		msg.TimeRequestFinished = requestFinished
	}

	if err != nil {
		var amqpErr *amqp.Error
		if errors.As(err, &amqpErr) {
			if amqpErr.Condition == amqp.ErrCondMessageSizeExceeded || amqpErr.Condition == amqp.ErrCondTransferLimitExceeded {
				return models.NewTargetWriteResult(nil, messages, nil), models.FatalWriteError{Err: errors.Wrap(err, "Unexpected oversized response from EventHubs")}
			}
		}

		// If we hit any error, we can't distinguish successful messages from failed ones,
		// so return all as failed.
		eht.log.Debugf("Failed to write %d messages", len(messages))
		return models.NewTargetWriteResult(nil, messages, nil), errors.Wrap(err, "Failed to send message batch to EventHub")
	}

	// If no error, all messages were successful
	for _, msg := range messages {
		if msg.AckFunc != nil {
			msg.AckFunc()
		}
	}

	eht.log.Debugf("Successfully wrote %d messages", len(messages))
	return models.NewTargetWriteResult(messages, nil, nil), nil
}

// Open does not do anything for this target
func (eht *EventHubTargetDriver) Open() error {
	return nil
}

// Close closes the eventhub client.
func (eht *EventHubTargetDriver) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(10)*time.Second)
	defer cancel()
	if err := eht.client.Close(ctx); err != nil {
		log.WithError(err).Error("failed to close eventHubTarget")
	}
}
