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
	"sync"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/snowplow/snowbridge/v3/config"
	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/source/sourceiface"
)

const SupportedSourceHTTP = "http"

// Configuration configures the source for records
type Configuration struct {
	ConcurrentWrites int    `hcl:"concurrent_writes,optional"`
	Path             string `hcl:"path,optional"`
	URL              string `hcl:"url"`
}

// httpSource holds a new HTTP server for accepting messages
type httpSource struct {
	sourceiface.NoOpObserver
	concurrentWrites int
	url              string
	path             string
	server           *http.Server
	cancel           context.CancelFunc

	log *log.Entry
}

// configFunction returns an http source from a config
func configfunction(c *Configuration) (sourceiface.Source, error) {
	return newHttpSource(c)
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
		ConcurrentWrites: 50,
		Path:             "/",
	}

	return cfg, nil
}

// adapterGenerator returns a HTTPSource adapter.
func adapterGenerator(f func(c *Configuration) (sourceiface.Source, error)) adapter {
	return func(i any) (any, error) {
		cfg, ok := i.(*Configuration)
		if !ok {
			return nil, errors.New("invalid input, expected HttpSourceConfig")
		}

		return f(cfg)
	}
}

// ConfigPair is passed to configuration to determine when to build an http source.
var ConfigPair = config.ConfigurationPair{
	Name:   SupportedSourceHTTP,
	Handle: adapterGenerator(configfunction),
}

// newHttpSource creates a new http server for accepting messages
func newHttpSource(c *Configuration) (*httpSource, error) {
	// Ensures as even as possible distribution of UUIDs
	uuid.EnableRandPool()

	return &httpSource{
		concurrentWrites: c.ConcurrentWrites,
		url:              c.URL,
		path:             c.Path,
		log:              log.WithFields(log.Fields{"source": SupportedSourceHTTP}),
	}, nil
}

// Read initializes HTTP server and starts accepting messages
func (hs *httpSource) Read(sf *sourceiface.SourceFunctions) error {
	hs.log.Infof("Starting HTTP source on: %s%s", hs.url, hs.path)

	throttle := make(chan struct{}, hs.concurrentWrites)
	wg := sync.WaitGroup{}

	ctx, cancel := context.WithCancel(context.Background())
	hs.cancel = cancel

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
			}
			messages = append(messages, message)
		}

		if err := scanner.Err(); err != nil {
			hs.log.WithError(err).Error("failed to scan request body")
			http.Error(w, "error processing request body", http.StatusBadRequest)
			return
		}

		if len(messages) > 0 {
			throttle <- struct{}{}
			wg.Go(func() {
				err := sf.WriteToTarget(messages)
				if err != nil {
					hs.log.WithError(err).Error(err)
				}
				<-throttle
			})
		}

		w.WriteHeader(http.StatusAccepted)
	})

	hs.server = &http.Server{
		Addr:    hs.url,
		Handler: mux,
	}

	// // Start server in a goroutine
	go func() {
		if err := hs.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			hs.log.WithError(err).Error("HTTP server error")
			hs.cancel()
		}
	}()

	// Wait for cancellation
	<-ctx.Done()

	// Wait for all goroutines to finish
	wg.Wait()
	return nil
}

// Stop cancels the http source receiver
func (hs *httpSource) Stop() {
	hs.log.Info("Stopping HTTP source")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := hs.server.Shutdown(ctx); err != nil {
		hs.log.WithError(err).Errorf("error during shutdown http server")
		if sErr := hs.server.Close(); sErr != nil {
			hs.log.WithError(sErr).Errorf("error during closing http server")
		}
	}

	hs.cancel()
}

// GetID returns the identifier for this source
func (hs *httpSource) GetID() string {
	return SupportedSourceHTTP
}
