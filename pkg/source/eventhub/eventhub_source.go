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

package eventhubsource

import (
	"context"
	"fmt"
	"os"
	"time"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/models"
	"github.com/snowplow/snowbridge/pkg/source/sourceiface"
)

// Configuration configures the source for records
type Configuration struct {
	EventHubNamespace string `hcl:"namespace"`
	EventHubName      string `hcl:"name"`

	ConcurrentWrites int `hcl:"concurrent_writes,optional"`
}

// eventHubSource holds a new client for reading messages from Apache Kafka
type eventHubSource struct {
	concurrentWrites  int
	eventHubNamespace string
	eventHubName      string
	log               *log.Entry
	cancel            context.CancelFunc

	client eventhubIface
}

type eventhubIface interface {
	Receive(ctx context.Context, partitionID string, handler eventhub.Handler, opts ...eventhub.ReceiveOption) (*eventhub.ListenerHandle, error)
	GetRuntimeInformation(context.Context) (*eventhub.HubRuntimeInformation, error)
	Close(context.Context) error
}

// Read initializes the Kafka consumer group and starts the message consumption loop
func (eh *eventHubSource) Read(sf *sourceiface.SourceFunctions) error {
	eh.log.Info("Reading messages from eventhub...")

	ctx, cancel := context.WithCancel(context.Background())
	// store reference to context cancel
	eh.cancel = cancel
	defer eh.client.Close(ctx)

	handler := func(c context.Context, event *eventhub.Event) error {
		var messages []*models.Message

		eh.log.Debugf("Read message with id: %s", event.ID)
		newMessage := &models.Message{
			Data:         event.Data,
			PartitionKey: uuid.New().String(),
			TimeCreated:  *event.SystemProperties.EnqueuedTime,
			TimePulled:   time.Now().UTC(),
		}

		messages = append(messages, newMessage)
		if err := sf.WriteToTarget(messages); err != nil {
			// When WriteToTarget returns an error it just means we failed to send some data -
			// these messages won't have been acked, so they'll get retried eventually.
			eh.log.WithFields(log.Fields{"error": err}).Error(err)
		}

		return nil
	}

	// listen to each partition of the Event Hub
	runtimeInfo, err := eh.client.GetRuntimeInformation(ctx)
	if err != nil {
		return err
	}

	for {
		for _, partitionID := range runtimeInfo.PartitionIDs {
			_, err := eh.client.Receive(ctx, partitionID, handler)
			if err != nil {
				return err
			}
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			eh.log.WithFields(log.Fields{"error": ctxErr}).Error(ctxErr)
			// ignore this error, it is called by cancelled context (on application exit)
			return nil
		}
	}
}

// Stop cancels the source receiver
func (eh *eventHubSource) Stop() {
	if eh.cancel != nil {
		eh.log.Warn("Cancelling Kafka receiver...")
		eh.cancel()
	}
	eh.cancel = nil
}

// adapterGenerator returns a Kafka Source adapter.
func adapterGenerator(f func(c *Configuration) (sourceiface.Source, error)) adapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*Configuration)
		if !ok {
			return nil, errors.New("invalid input, expected EventHubSourceConfig")
		}

		return f(cfg)
	}
}

// ConfigPair is passed to configuration to determine when to build a Kafka source.
var ConfigPair = config.ConfigurationPair{
	Name:   "eventHub",
	Handle: adapterGenerator(configFunction),
}

// configFunction returns a eventHub source from a config
func configFunction(c *Configuration) (sourceiface.Source, error) {
	return newEventHubSource(c)
}

// The adapter type is an adapter for functions to be used as
// pluggable components for Kafka Source. It implements the Pluggable interface.
type adapter func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface.
func (f adapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface
func (f adapter) ProvideDefault() (interface{}, error) {
	// Provide defaults
	cfg := &Configuration{
		ConcurrentWrites: 15,
	}

	return cfg, nil
}

// newEventHubSource creates a new hanlder for reading messages from Azure EventHub
func newEventHubSource(cfg *Configuration) (*eventHubSource, error) {
	_, keyNamePresent := os.LookupEnv("EVENTHUB_KEY_NAME")
	_, keyValuePresent := os.LookupEnv("EVENTHUB_KEY_VALUE")

	connString, connStringPresent := os.LookupEnv("EVENTHUB_CONNECTION_STRING")

	_, tenantIDPresent := os.LookupEnv("AZURE_TENANT_ID")
	_, clientIDPresent := os.LookupEnv("AZURE_CLIENT_ID")

	_, clientSecretPresent := os.LookupEnv("AZURE_CLIENT_SECRET")

	_, azCertPathPresent := os.LookupEnv("AZURE_CERTIFICATE_PATH")
	_, azCertPwrdPresent := os.LookupEnv("AZURE_CERTIFICATE_PASSWORD")

	if !(connStringPresent || (keyNamePresent && keyValuePresent) || (tenantIDPresent && clientIDPresent && ((azCertPathPresent && azCertPwrdPresent) || clientSecretPresent))) {
		return nil, errors.Errorf("Error initialising EventHub client: No valid combination of authentication Env vars found. https://pkg.go.dev/github.com/Azure/azure-event-hubs-go#NewHubWithNamespaceNameAndEnvironment")
	}

	var hub *eventhub.Hub
	var err error
	if connStringPresent {
		hub, err = eventhub.NewHubFromConnectionString(connString)
	} else {
		hub, err = eventhub.NewHubWithNamespaceNameAndEnvironment(cfg.EventHubNamespace, cfg.EventHubName)
	}

	if err != nil {
		return nil, err
	}

	logger := log.WithFields(log.Fields{
		"source":            "eventhub",
		"eventHubNamespace": cfg.EventHubNamespace,
		"eventHubName":      cfg.EventHubName,
	})

	return newEventHubSourceWithInterfaces(hub, &eventHubSource{
		eventHubNamespace: cfg.EventHubNamespace,
		eventHubName:      cfg.EventHubName,
		log:               logger,
		concurrentWrites:  cfg.ConcurrentWrites,
	})
}

// newEventHubSourceWithInterfaces creates a new source for reading messages from Apache Kafka, allowing the user to provide a mocked client.
func newEventHubSourceWithInterfaces(client eventhubIface, s *eventHubSource) (*eventHubSource, error) {
	// Ensures as even as possible distribution of UUIDs
	uuid.EnableRandPool()
	s.client = client
	return s, nil
}

// GetID returns the identifier for this target
func (ks *eventHubSource) GetID() string {
	return fmt.Sprintf("namespace:%s:name:%s", ks.eventHubNamespace, ks.eventHubName)
}
