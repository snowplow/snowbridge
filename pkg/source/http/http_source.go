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
	"bufio"
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/snowplow/snowbridge/v3/config"
	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/source/sourceiface"
)

// Configuration configures the HTTP source
type Configuration struct {
	Port int `hcl:"port,optional"`
}

// httpSource holds an HTTP server for receiving messages via POST requests
type httpSource struct {
	sourceiface.NoOpObserver
	port     int
	server   *http.Server
	stopChan chan struct{}
	wg       sync.WaitGroup

	log *log.Entry
}

// configFunction returns an HTTP source from a config
func configfunction(c *Configuration) (sourceiface.Source, error) {
	return newHTTPSource(c.Port)
}

// The adapter type is an adapter for functions to be used as
// pluggable components for HTTP Source. It implements the Pluggable interface.
type adapter func(i any) (any, error)

// Create implements the ComponentCreator interface.
func (f adapter) Create(i any) (any, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface.
func (f adapter) ProvideDefault() (any, error) {
	// Provide defaults
	cfg := &Configuration{
		Port: 8080,
	}

	return cfg, nil
}

// adapterGenerator returns an HTTP Source adapter.
func adapterGenerator(f func(c *Configuration) (sourceiface.Source, error)) adapter {
	return func(i any) (any, error) {
		cfg, ok := i.(*Configuration)
		if !ok {
			return nil, errors.New("invalid input, expected HTTPSourceConfig")
		}

		return f(cfg)
	}
}

// ConfigPair is passed to configuration to determine when to build an HTTP source.
var ConfigPair = config.ConfigurationPair{
	Name:   "http",
	Handle: adapterGenerator(configfunction),
}

// newHTTPSource creates a new HTTP source for receiving messages via POST requests
func newHTTPSource(port int) (*httpSource, error) {
	// Ensures as even as possible distribution of UUIDs
	uuid.EnableRandPool()

	return &httpSource{
		port:     port,
		stopChan: make(chan struct{}),
		log:      log.WithFields(log.Fields{"source": "http"}),
	}, nil
}

// Read starts the HTTP server and listens for POST requests
func (hs *httpSource) Read(sf *sourceiface.SourceFunctions) error {
	hs.log.Infof("Starting HTTP source on port %d", hs.port)

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			hs.log.WithError(err).Error("Failed to read request body")
			http.Error(w, "Failed to read request body", http.StatusBadRequest)
			return
		}

		// Process each line in the request body as a separate message
		scanner := bufio.NewScanner(strings.NewReader(string(body)))
		var messages []*models.Message
		timeNow := time.Now().UTC()

		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				continue
			}

			message := &models.Message{
				Data:         []byte(line),
				PartitionKey: uuid.New().String(),
				TimeCreated:  timeNow,
				TimePulled:   timeNow,
				AckFunc:      func() {}, // No-op for HTTP source
			}
			messages = append(messages, message)
		}

		if err := scanner.Err(); err != nil {
			hs.log.WithError(err).Error("Error scanning request body")
			http.Error(w, "Error processing request body", http.StatusBadRequest)
			return
		}

		if len(messages) > 0 {
			hs.wg.Add(1)
			go func() {
				defer hs.wg.Done()

				err := sf.WriteToTarget(messages)
				if err != nil {
					hs.log.WithError(err).Error("Failed to write messages to target")
				}
			}()
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK\n"))
	})

	hs.server = &http.Server{
		Addr:    ":" + strconv.Itoa(hs.port),
		Handler: mux,
	}

	// Start server in a goroutine
	go func() {
		if err := hs.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			hs.log.WithError(err).Error("HTTP server error")
		}
	}()

	// Wait for stop signal
	<-hs.stopChan

	// Wait for all goroutines to finish
	hs.wg.Wait()

	return nil
}

// Stop gracefully shuts down the HTTP server
func (hs *httpSource) Stop() {
	hs.log.Info("Stopping HTTP source")

	if hs.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := hs.server.Shutdown(ctx); err != nil {
			hs.log.WithError(err).Error("Error shutting down HTTP server")
		}
	}

	close(hs.stopChan)
}

// GetID returns the identifier for this source
func (hs *httpSource) GetID() string {
	return "http"
}