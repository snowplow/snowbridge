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

package cli

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"

	// pprof imported for the side effect of registering its HTTP handlers
	_ "net/http/pprof"

	"github.com/snowplow/snowbridge/v3/cmd"
	"github.com/snowplow/snowbridge/v3/config"
	"github.com/snowplow/snowbridge/v3/pkg/source/sourceconfig"
	"github.com/snowplow/snowbridge/v3/pkg/target/targetconfig"
	"github.com/snowplow/snowbridge/v3/pkg/telemetry"
	"github.com/snowplow/snowbridge/v3/pkg/transform/transformconfig"
)

// RunApp runs application
func RunApp(cfg *config.Config, supportedTransformations []config.ConfigurationPair) error {

	// Create channels
	invalidChannel := make(chan *invalidMessages)

	// First thing is to spin up webhookMonitoring, so we can start alerting as soon as possible
	webhookMonitoring, alertChan, err := cfg.GetWebhookMonitoring(cmd.AppName, cmd.AppVersion)
	if err != nil {
		return err
	}
	if webhookMonitoring != nil {
		defer webhookMonitoring.Stop()
		webhookMonitoring.Start()
	}

	tags, err := cfg.GetTags()
	if err != nil {
		return err
	}

	obs, err := cfg.GetObserver(cmd.AppName, cmd.AppVersion, tags)
	if err != nil {
		return err
	}
	obs.Start()
	defer obs.Stop()

	source, sourceOutput, err := sourceconfig.GetSource(cfg, obs)
	if err != nil {
		return err
	}

	transformer, transformationOutput, err := transformconfig.GetTransformer(cfg, supportedTransformations, sourceOutput, obs)
	if err != nil {
		return err
	}

	target, err := targetconfig.GetTarget(cfg.Data.Target, cfg.Decoder)
	if err != nil {
		return err
	}

	filterTarget, err := targetconfig.GetTarget(cfg.Data.FilterTarget, cfg.Decoder)
	if err != nil {
		return err
	}

	failureTarget, err := targetconfig.GetTarget(cfg.Data.FailureTarget, cfg.Decoder)
	if err != nil {
		return err
	}

	// Get failure parser based on config and failure target max message size
	failureParser, err := cfg.GetFailureParser(failureTarget.GetBatchingConfig().MaxMessageBytes, cmd.AppName, cmd.AppVersion)
	if err != nil {
		return err
	}

	stopTelemetry := telemetry.InitTelemetryWithCollector(cfg)
	defer stopTelemetry()

	// Build context that is then passed to the source.
	// Listed OS signals cancel context.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	// Create Router to orchestrate data flow from transformation to targets
	router := &Router{
		transformationOutput: transformationOutput,
		invalidChannel:       invalidChannel,
		AlertChannel:         alertChan,
		cancel:               cancel,

		// Targets (all use targetiface.Target)
		Target:        target,
		FilterTarget:  filterTarget,
		FailureTarget: failureTarget,

		FailureParser: failureParser,
		metrics:       obs,
		maxTargetSize: target.GetBatchingConfig().MaxMessageBytes,
		retryConfig:   cfg.Data.Retry,
	}

	var wg sync.WaitGroup

	// Start all async components.
	// If any of them quits naturally, without any error, cancel context to signal we should shut down application.
	runAsync := func(start func()) {
		wg.Go(func() {
			defer cancel()
			start()
		})
	}

	runAsync(func() { source.Start(ctx) })
	runAsync(transformer.Start)
	runAsync(router.Start)

	// Wait for context cancellation, might be caused by:
	// - OS signal
	// - Component calling cancel() due to fatal error
	// - Component quits naturally
	<-ctx.Done()

	log.Info("Starting graceful shutdown. Waiting for app to complete shutdown...")
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Info("App shutdown completed successfully")
	case <-time.After(5 * time.Second):
		log.Warn("Shutdown timed out after 5 seconds, forcing quit...")
	}

	return err
}
