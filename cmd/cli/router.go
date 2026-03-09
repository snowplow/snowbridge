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
	"sync"
	"time"

	retry "github.com/avast/retry-go/v4"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/snowplow/snowbridge/v3/config"
	"github.com/snowplow/snowbridge/v3/pkg/failure"
	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/target/targetiface"
)

// invalidMessages holds invalid and oversized messages to pass through the invalid channel
type invalidMessages struct {
	Invalid   []*models.Message
	Oversized []*models.Message
}

// RouterMetrics defines the methods needed for router metrics tracking
type RouterMetrics interface {
	TargetWrite(r *models.TargetWriteResult)
	TargetWriteInvalid(r *models.TargetWriteResult)
	TargetWriteFiltered(r *models.TargetWriteResult)
}

// Router orchestrates data flow from transformation channel to targets
type Router struct {
	// Input: single channel from transformation
	transformationOutput chan *models.TransformationResult

	// Output: invalid channel for invalid/oversized
	invalidChannel chan *invalidMessages

	AlertChannel chan error

	// Function to cancel application context on fatal errors
	cancel context.CancelFunc

	// Targets (state now lives in targets themselves via embedded BatchingState)
	Target        *targetiface.Target
	FilterTarget  *targetiface.Target
	FailureTarget *targetiface.Target

	FailureParser failure.FailureParser

	// Metrics for tracking router operations
	metrics RouterMetrics

	// For getting max size for oversized messages
	maxTargetSize int

	// Retry configuration
	retryConfig *config.RetryConfig
}

func (r *Router) Start() {
	// Start Router goroutines to orchestrate data flow
	wg := sync.WaitGroup{}
	wg.Go(r.Route)        // Handles good and filtered data
	wg.Go(r.RouteInvalid) // Handles invalid and oversized data

	// Return from start only after both routers finish
	wg.Wait()
}

// Route deals with the different outcomes of transformation
// For the good and filter targets, it performs batching logic and spawns writes for ready batches
// Then it passes invalids and oversized data to the invalid channel
func (r *Router) Route() {
	defer r.goodRouterShutdown()

	if err := r.Target.Open(); err != nil {
		log.WithError(err).Error("Failed to open target")
		r.cancel()
		return
	}
	if err := r.FilterTarget.Open(); err != nil {
		log.WithError(err).Error("Failed to open filter target")
		r.cancel()
		return
	}

	for {
		select {
		// Flush on ticker
		// For simplicity, we only care about the good target's ticker
		// Filtered gets flushed whenever that happens
		case <-r.Target.Ticker.C:
			r.flushGoodBuffer(r.Target, r.metrics.TargetWrite)
			r.flushGoodBuffer(r.FilterTarget, r.metrics.TargetWriteFiltered)

		case messages, ok := <-r.transformationOutput:
			if !ok {
				log.Info("Transformation output channel closed")
				return
			}

			// Pass invalid data to invalid channel (blocks if full - backpressure)
			if messages.Invalid != nil {
				r.invalidChannel <- &invalidMessages{Invalid: []*models.Message{messages.Invalid}}
			}

			r.handleGoodMessages(messages)
			r.handleFilteredMessages(messages)
		}
	}
}

// RouteInvalid handles invalid and oversized data
// It creates failure payloads and batches them before writing to the failure target
func (r *Router) RouteInvalid() {
	defer r.invalidRouterShutdown()

	if err := r.FailureTarget.Open(); err != nil {
		log.WithError(err).Error("Failed to open failure target")
		r.cancel()
		return
	}

	for {
		select {
		// Flush on ticker
		case <-r.FailureTarget.Ticker.C:
			r.flushFailureBuffer()

		case messages, ok := <-r.invalidChannel:
			if !ok {
				log.Info("Invalid channel closed")
				return
			}

			r.handleInvalidMessages(messages)
		}
	}
}

// WriteBatch deals with writing a single batch to a (non-failure) target
func (r *Router) WriteBatch(batch []*models.Message, target *targetiface.Target, metricsFunc func(*models.TargetWriteResult)) {
	target.SpawnThrottledAsyncWrite(func() {
		var writeResult *models.TargetWriteResult

		invalids := make([]*models.Message, 0)
		messagesToSend := batch
		writeFunc := func() error {
			var err error
			writeResult, err = target.Write(messagesToSend)
			metricsFunc(writeResult)

			messagesToSend = writeResult.Failed
			invalids = append(invalids, writeResult.Invalid...)
			return err
		}

		err, sendToInvalid := handleWriteWithRetryConfig(r.retryConfig, writeFunc, r.AlertChannel)

		if err != nil {
			if sendToInvalid {
				// After max retries, send failed messages to invalid channel
				log.WithError(err).Warn("Target write failed after retries, sending failed messages to invalid")
				if writeResult != nil {
					if len(writeResult.Failed) > 0 {
						invalids = append(invalids, writeResult.Failed...)
					}
				}
			} else {
				err = errors.Wrap(err, "Target write failed after retries")
				r.signalUnrecoverableError(err, writeResult.Failed)
			}
		}

		// Pass any invalid results to the invalid channel
		if len(invalids) > 0 {
			r.invalidChannel <- &invalidMessages{Invalid: invalids}
		}
	})
}

// WriteFailureBatch writes a batch to the failure target
func (r *Router) WriteFailureBatch(batch []*models.Message, metricsFunc func(*models.TargetWriteResult)) {
	r.FailureTarget.SpawnThrottledAsyncWrite(func() {
		var writeResult *models.TargetWriteResult

		invalids := make([]*models.Message, 0)
		messagesToSend := batch
		writeFunc := func() error {
			var err error
			writeResult, err = r.FailureTarget.Write(messagesToSend)
			metricsFunc(writeResult)

			messagesToSend = writeResult.Failed
			invalids = append(invalids, writeResult.Invalid...)
			return err
		}

		err := handleSimpleWrite(writeFunc)
		if err != nil {
			err = errors.Wrap(err, "Failure target write failed")
			r.signalUnrecoverableError(err, writeResult.Failed)
		}

		if len(invalids) > 0 {
			err = errors.New("Failure target produced invalid messages - this should not happen")
			r.signalUnrecoverableError(err, invalids)
		}
	})
}

func (r *Router) handleGoodMessages(messages *models.TransformationResult) {
	if messages.Transformed != nil {
		batchToSend, oversized := r.Target.AddMessage(messages.Transformed)

		// If we have a batch ready, send it
		if batchToSend != nil {
			r.WriteBatch(batchToSend, r.Target, r.metrics.TargetWrite)
		}

		// Pass oversized data to invalid channel (blocks if full - backpressure)
		if oversized != nil {
			r.invalidChannel <- &invalidMessages{Oversized: []*models.Message{oversized}}
		}
	}
}

func (r *Router) handleFilteredMessages(messages *models.TransformationResult) {
	if messages.Filtered != nil {
		batchToFilter, oversized := r.FilterTarget.AddMessage(messages.Filtered)

		if batchToFilter != nil {
			r.WriteBatch(batchToFilter, r.FilterTarget, r.metrics.TargetWriteFiltered)
		}

		// Pass  oversized data to invalid channel (blocks if full - backpressure)
		if oversized != nil {
			r.invalidChannel <- &invalidMessages{Oversized: []*models.Message{oversized}}
		}
	}
}

func (r *Router) goodRouterShutdown() {
	log.Info("Flushing and shutting down good router")

	// Write any current batches
	r.flushGoodBuffer(r.Target, r.metrics.TargetWrite)
	r.flushGoodBuffer(r.FilterTarget, r.metrics.TargetWriteFiltered)

	// Wait for everything that can output to invalid
	r.Target.WaitGroup.Wait()
	r.FilterTarget.WaitGroup.Wait()

	log.Info("Closing target and filter target...")
	r.Target.Close()
	r.FilterTarget.Close()

	// Close the invalid channel
	close(r.invalidChannel)
}

func (r *Router) invalidRouterShutdown() {
	log.Info("Flushing and shutting down failure router")
	r.flushFailureBuffer()

	r.FailureTarget.WaitGroup.Wait()

	log.Info("Closing failure target...")
	r.FailureTarget.Close()
}

func (r *Router) handleInvalidMessages(messages *invalidMessages) {
	genericInvalids, err := r.FailureParser.MakeInvalidPayloads(messages.Invalid)
	if err != nil {
		err = errors.Wrap(err, "Failed to transform invalid messages")
		r.signalUnrecoverableError(err, messages.Invalid)
		return
	}

	oversizedInvalids, err := r.FailureParser.MakeOversizedPayloads(r.maxTargetSize, messages.Oversized)
	if err != nil {
		err = errors.Wrap(err, "Failed to transform oversized messages")
		r.signalUnrecoverableError(err, messages.Oversized)
		return
	}

	allInvalids := append(genericInvalids, oversizedInvalids...)

	// This target does receive batches of data (which writes can produce), so we iterate and deal with them one by one
	for _, invalid := range allInvalids {
		invalidBatchToSend, invalidOversized := r.FailureTarget.AddMessage(invalid)
		// If we have a batch ready, send it
		if invalidBatchToSend != nil {
			r.WriteFailureBatch(invalidBatchToSend, r.metrics.TargetWriteInvalid)
		}
		// If we get an oversized, something unexpected has gone wrong.
		if invalidOversized != nil {
			err = errors.New("Invalid payloads should already be trimmed to not exceed failure target's limits")
			r.signalUnrecoverableError(err, []*models.Message{invalidOversized})
		}
	}

}

// handleWriteWithRetryConfig wraps each target write operation with 3 kinds of retries:
// - setup errors (errors which may be observed when target is misconfigured):
// -- configurable retry attempts + alerts
// - throttle errors (errors which may be observed when we hit 429 error of the target):
// -- configurable retry attempts, no alerts
// - transient errors (any other error not of the above types):
// -- configurable retry attempts, no alerts
// Type of an error is decided based on a response returned by the target.
func handleWriteWithRetryConfig(cfg *config.RetryConfig, write func() error, alertChan chan error) (err error, sendToInvalid bool) {
	setupErrored := false

	retryOnlySetupErrors := retry.RetryIf(func(err error) bool {
		_, isSetup := err.(models.SetupWriteError)
		return isSetup
	})

	onSetupError := retry.OnRetry(func(attempt uint, err error) {
		log.Warnf("Setup target write error. Attempt: %d, error: %s\n", attempt+1, err)
		if alertChan != nil {
			setupErrored = true
			alertChan <- err
		}
	})

	//First try to handle error as setup...
	err = retry.Do(
		write,
		retryOnlySetupErrors,
		onSetupError,
		retry.Delay(time.Duration(cfg.Setup.Delay)*time.Millisecond),
		retry.Attempts(uint(cfg.Setup.MaxAttempts)),
		retry.LastErrorOnly(true),
	)

	// If after retries we still have setup error
	// there is no reason to retry it further, so error early
	if _, isSetup := err.(models.SetupWriteError); isSetup {
		return err, cfg.Setup.InvalidAfterMax
	}

	if _, isFatal := err.(models.FatalWriteError); isFatal {
		log.WithError(err).Error("Fatal write error detected, shutting down")
		return err, false
	}

	// Now, `err` is either nil or no longer setup-related
	// Thus we should reset monitoring to re-enable heartbeats
	if alertChan != nil && setupErrored {
		alertChan <- nil
	}

	if err == nil {
		return err, false
	}

	// If no setup, then check if it is throttle
	if _, isThrottle := err.(models.ThrottleWriteError); isThrottle {
		log.Warnf("Throttle target write error. Starting retrying. error: %s\n", err)
		// We already had at least 1 attempt from above 'setup' retrying section,
		// so before we start throttle retrying we need to add 'manual' initial delay.
		time.Sleep(time.Duration(cfg.Throttle.Delay) * time.Millisecond)

		retryOnlyThrottleErrors := retry.RetryIf(func(err error) bool {
			_, isThrottle := err.(models.ThrottleWriteError)
			return isThrottle
		})
		onThrottleError := retry.OnRetry(func(retry uint, err error) {
			log.Warnf("Retry failed with throttle error. Retry counter: %d, error: %s\n", retry+1, err)
		})

		err = retry.Do(
			write,
			retryOnlyThrottleErrors,
			onThrottleError,
			retry.Delay(time.Duration(cfg.Throttle.Delay)*time.Millisecond),
			retry.Attempts(uint(cfg.Throttle.MaxAttempts)),
			retry.LastErrorOnly(true),
		)
	}

	// If after retries we still have throttle error
	// there is no reason to retry it further, so error early
	if _, isThrottle := err.(models.ThrottleWriteError); isThrottle {
		return err, cfg.Throttle.InvalidAfterMax
	}

	if err == nil {
		return err, false
	}

	// If no throttle, then handle as transient.
	log.Warnf("Transient target write error. Starting retrying. error: %s\n", err)
	// We already had at least 1 attempt from above 'throttle' retrying section,
	// so before we start transient retrying we need to add 'manual' initial delay.
	time.Sleep(time.Duration(cfg.Transient.Delay) * time.Millisecond)

	onTransientError := retry.OnRetry(func(retry uint, err error) {
		log.Warnf("Retry failed with transient error. Retry counter: %d, error: %s\n", retry+1, err)
	})

	retryOnlyNotFatal := retry.RetryIf(func(err error) bool {
		_, isFatal := err.(models.FatalWriteError)
		return !isFatal
	})

	err = retry.Do(
		write,
		retryOnlyNotFatal,
		onTransientError,
		// * 2 because we have initial sleep above
		retry.Delay(time.Duration(cfg.Transient.Delay*2)*time.Millisecond),
		retry.Attempts(uint(cfg.Transient.MaxAttempts)),
		retry.LastErrorOnly(true),
	)

	return err, cfg.Transient.InvalidAfterMax
}

// handleSimpleWrite wraps target write operation with a simple retry logic:
// - retry any error 5 times, 50ms between retries
// Main diff from `handleWriteWithRetryConfig` is that we retry regardless of the error type
// and don't produce alert load events.
func handleSimpleWrite(write func() error) error {
	onAnyError := retry.OnRetry(func(retry uint, err error) {
		_, isFatal := err.(models.FatalWriteError)
		if !isFatal {
			log.Warnf("Retry failed with an error. Retry counter: %d, error: %s\n", retry+1, err)
		}
	})

	retryIfNotFatal := retry.RetryIf(func(err error) bool {
		_, isFatal := err.(models.FatalWriteError)
		return !isFatal
	})

	err := retry.Do(
		write,
		onAnyError,
		retryIfNotFatal,
		retry.Delay(time.Duration(50)*time.Millisecond),
		retry.Attempts(5),
		retry.LastErrorOnly(true),
	)

	if _, isFatal := err.(models.FatalWriteError); isFatal {
		log.WithError(err).Error("Fatal write error detected, shutting down")
	}

	return err
}

func (r *Router) flushGoodBuffer(target *targetiface.Target, metricsFunc func(*models.TargetWriteResult)) {
	if messages := target.Flush(); messages != nil {
		r.WriteBatch(messages, target, metricsFunc)
	}
}

func (r *Router) flushFailureBuffer() {
	if messages := r.FailureTarget.Flush(); messages != nil {
		r.WriteFailureBatch(messages, r.metrics.TargetWriteInvalid)
	}
}

func (r *Router) signalUnrecoverableError(err error, toNack []*models.Message) {
	log.WithError(err).Error("Unrecoverable error in router")
	nackMessages(toNack)
	r.cancel()
}

func nackMessages(batch []*models.Message) {
	for _, msg := range batch {
		if msg.NackFunc != nil {
			msg.NackFunc()
		}
	}
}
