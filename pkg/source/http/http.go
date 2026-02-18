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
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/source/sourceiface"
)

// Note: This is an experimental source
//
// For now it is agreed that we allow a risk of a potential data loss
// of which user won't be acknowledged of.

const SupportedSourceHTTP = "http"

// Configuration configures the source for records
type Configuration struct {
	RequestBatchLimit int    `hcl:"request_batch_limit,optional"`
	Path              string `hcl:"path,optional"`
	URL               string `hcl:"url"`
}

// httpSourceDriver holds a new HTTP server for accepting messages
type httpSourceDriver struct {
	sourceiface.SourceChannels

	requestBatchLimit int
	url               string
	path              string
	server            *http.Server

	log *log.Entry
}

// DefaultConfiguration returns the default configuration for http source
func DefaultConfiguration() Configuration {
	return Configuration{
		RequestBatchLimit: 50,
		Path:              "/",
	}
}

// BuildFromConfig creates an HTTP source from decoded configuration
func BuildFromConfig(cfg *Configuration) (sourceiface.Source, error) {
	// Ensures as even as possible distribution of UUIDs
	uuid.EnableRandPool()

	return &httpSourceDriver{
		requestBatchLimit: cfg.RequestBatchLimit,
		url:               cfg.URL,
		path:              cfg.Path,
		log:               log.WithFields(log.Fields{"source": SupportedSourceHTTP}),
	}, nil
}

// Start initializes HTTP server and starts accepting messages
func (hs *httpSourceDriver) Start(ctx context.Context) {
	defer func() {
		if err := hs.server.Shutdown(context.Background()); err != nil {
			hs.log.WithError(err).Error("error during shutdown http server")
			if sErr := hs.server.Close(); sErr != nil {
				hs.log.WithError(sErr).Error("error during closing http server")
			}
		}

		close(hs.MessageChannel)
	}()

	hs.log.Infof("Starting HTTP source on: %s%s", hs.url, hs.path)

	mux := http.NewServeMux()
	mux.HandleFunc(hs.path, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			hs.log.Errorf("method not allowed: [%s] but expecting [%s]", r.Method, http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			hs.log.WithError(err).Error("failed to read request body")
			http.Error(w, "error reading body", http.StatusBadRequest)
			return
		}
		defer func() {
			if err := r.Body.Close(); err != nil {
				hs.log.WithError(err).Error("error while closing Body")
			}
		}()

		// Process each line in the request body as a separate message
		scanner := bufio.NewScanner(strings.NewReader(string(body)))
		timeNow := time.Now().UTC()

		processedCounter := 0
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				continue
			}

			if processedCounter > hs.requestBatchLimit {
				hs.log.Errorf("request batch limit is breached: [%d], limit is: [%d]", processedCounter, hs.requestBatchLimit)
				http.Error(w, "request batch limit is reached", http.StatusBadRequest)
				return
			}

			message := &models.Message{
				Data:         []byte(line),
				PartitionKey: uuid.New().String(),
				TimeCreated:  timeNow,
				TimePulled:   timeNow,
			}

			// Send message with context awareness
			select {
			case <-ctx.Done():
				http.Error(w, "server shutting down", http.StatusServiceUnavailable)
				return
			case hs.MessageChannel <- message:
			}
			processedCounter++
		}

		if err := scanner.Err(); err != nil {
			hs.log.WithError(err).Error("failed to scan request body")
			http.Error(w, "error processing request body", http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusAccepted)
	})

	hs.server = &http.Server{
		Addr:    hs.url,
		Handler: mux,
	}

	// Start server in a goroutine
	serverErr := make(chan error, 1)
	go func() {
		if err := hs.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	// Wait for context cancellation or server error
	select {
	case <-ctx.Done():
		hs.log.Info("Context cancelled, stopping HTTP source")
	case err := <-serverErr:
		hs.log.WithError(err).Error("HTTP server fails with an error")
	}
}
