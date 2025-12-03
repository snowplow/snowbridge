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
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/getsentry/sentry-go"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"net/http"
	// pprof imported for the side effect of registering its HTTP handlers
	_ "net/http/pprof"

	retry "github.com/avast/retry-go/v4"
	"github.com/snowplow/snowbridge/v3/cmd"
	"github.com/snowplow/snowbridge/v3/config"
	"github.com/snowplow/snowbridge/v3/pkg/failure/failureiface"
	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/observer"
	"github.com/snowplow/snowbridge/v3/pkg/source/sourceconfig"
	"github.com/snowplow/snowbridge/v3/pkg/source/sourceiface"
	"github.com/snowplow/snowbridge/v3/pkg/target/targetiface"
	"github.com/snowplow/snowbridge/v3/pkg/telemetry"
	"github.com/snowplow/snowbridge/v3/pkg/transform"
	"github.com/snowplow/snowbridge/v3/pkg/transform/transformconfig"
)

const (
	appVersion   = cmd.AppVersion
	appName      = cmd.AppName
	appUsage     = "Replicates data streams to supported targets"
	appCopyright = "(c) 2020-present Snowplow Analytics Ltd. All rights reserved."
)

// RunCli allows running application from cli
func RunCli(supportedSources []config.ConfigurationPair, supportedTransformations []config.ConfigurationPair) {
	config, sentryEnabled, err := cmd.Init()
	if err != nil {
		exitWithError(err, sentryEnabled)
	}
	app := cli.NewApp()
	app.Name = appName
	app.Usage = appUsage
	app.Version = appVersion
	app.Copyright = appCopyright
	app.Compiled = time.Now().UTC()
	app.Authors = []cli.Author{
		{
			Name:  "Joshua Beemster",
			Email: "support@snowplow.io",
		},
		{
			Name:  "Colm O Griobhtha",
			Email: "support@snowplow.io",
		},
	}

	app.Flags = []cli.Flag{
		cli.BoolFlag{
			Name:  "profile, p",
			Usage: "Enable application profiling endpoint on port 8080",
		},
	}

	app.Action = func(c *cli.Context) error {
		profile := c.Bool("profile")
		if profile {
			go func() {
				if err := http.ListenAndServe("localhost:8080", nil); err != nil {
					log.WithError(err).Fatal("failed to start up the server")
				}
			}()
		}
		return RunApp(config, supportedSources, supportedTransformations)
	}

	app.ExitErrHandler = func(context *cli.Context, err error) {
		if err != nil {
			exitWithError(err, sentryEnabled)
		}
	}

	if err := app.Run(os.Args); err != nil {
		log.WithError(err).Error("failed to run cli")
	}
}

// RunApp runs application (without cli stuff)
func RunApp(cfg *config.Config, supportedSources []config.ConfigurationPair, supportedTransformations []config.ConfigurationPair) error {
	// First thing is to spin up webhookMonitoring, so we can start alerting as soon as possible
	webhookMonitoring, alertChan, err := cfg.GetWebhookMonitoring(cmd.AppName, cmd.AppVersion)
	if err != nil {
		return err
	}
	if webhookMonitoring != nil {
		defer webhookMonitoring.Stop()
		webhookMonitoring.Start()
	}

	source, err := sourceconfig.GetSource(cfg, supportedSources)
	if err != nil {
		return err
	}

	transformations, err := transformconfig.GetTransformations(cfg, supportedTransformations)
	if err != nil {
		return err
	}

	target, err := cfg.GetTarget()
	if err != nil {
		return err
	}
	target.Open()

	failureTarget, err := cfg.GetFailureTarget(cmd.AppName, cmd.AppVersion)
	if err != nil {
		return err
	}
	failureTarget.Open()

	filterTarget, err := cfg.GetFilterTarget()
	if err != nil {
		return err
	}
	filterTarget.Open()

	tags, err := cfg.GetTags()
	if err != nil {
		return err
	}

	obs, err := cfg.GetObserver(cmd.AppName, cmd.AppVersion, tags)
	if err != nil {
		return err
	}
	obs.Start()

	// Add observer to source, so source metrics can be configured
	source.SetObserver(obs)

	stopTelemetry := telemetry.InitTelemetryWithCollector(cfg)

	// Handle SIGTERM
	sig := make(chan os.Signal)
	// TODO: below could be reworked to use signal.NotifyContext, but would require a bit of testing
	// nolint: govet,staticcheck
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM, os.Kill)
	go func() {
		<-sig
		log.Warn("SIGTERM called, cleaning up and closing application ...")

		stop := make(chan struct{}, 1)
		go func() {
			source.Stop()
			stop <- struct{}{}
		}()

		select {
		case <-stop:
			log.Debug("source.Stop() finished successfully!")

			stopTelemetry()
			if err != nil {
				log.Debugf(`error deleting tmp directory: %v`, err)
			}
		case <-time.After(5 * time.Second):
			log.Error("source.Stop() took more than 5 seconds, forcing shutdown ...")

			target.Close()
			failureTarget.Close()
			filterTarget.Close()
			obs.Stop()
			stopTelemetry()

			if err != nil {
				log.Debugf(`error deleting tmp directory: %v`, err)
			}

			os.Exit(1)
		}
	}()

	// Callback functions for the source to leverage when writing data
	sf := sourceiface.SourceFunctions{
		WriteToTarget: sourceWriteFunc(target, failureTarget, filterTarget, transformations, obs, cfg, alertChan),
	}

	// Read is a long running process and will only return when the source
	// is exhausted or if an error occurs
	err = source.Read(&sf)
	if err != nil {
		return err
	}

	target.Close()
	failureTarget.Close()
	filterTarget.Close()
	obs.Stop()
	return nil
}

// sourceWriteFunc builds the function which wraps the different objects together to handle:
//
// 1. Sending messages to the target
// 2. Observing results
// 3. Sending oversized messages to the failure target
// 4. Observing these results
//
// All with retry logic baked in to remove any of this handling from the implementations
func sourceWriteFunc(t targetiface.Target, ft failureiface.Failure, filter targetiface.Target, tr transform.TransformationApplyFunction, o *observer.Observer, cfg *config.Config, alertChan chan error) func(messages []*models.Message) error {
	return func(messages []*models.Message) error {

		copyOriginalData(messages)

		// Apply transformations
		transformed := tr(messages)
		// no error as errors should be returned in the failures array of TransformationResult

		// Send message buffer
		invalid := transformed.Invalid
		var messagesToSend []*models.Message
		var oversized []*models.Message

		if len(transformed.Result) > 0 {
			messagesToSend = transformed.Result
			writeTransformed := func() error {
				result, err := t.Write(messagesToSend)

				o.TargetWrite(result)
				messagesToSend = result.Failed
				oversized = append(oversized, result.Oversized...)
				invalid = append(invalid, result.Invalid...)
				return err
			}

			err, sendToInvalid := handleWrite(cfg, writeTransformed, alertChan)
			if err != nil && !sendToInvalid {
				// Return error and crash if configured to do so.
				return err
			}
			// If we get here, we either have empty messagesToSend (as all successful),
			// or we have configured to send the data to invalid after max retries.
			// (the write function overwrites messagesToSend with failed on each iteration)
			invalid = append(invalid, messagesToSend...)
		}

		if len(transformed.Filtered) > 0 {
			messagesToSend = transformed.Filtered
			writeFiltered := func() error {
				result, err := filter.Write(messagesToSend)
				filterRes := models.NewFilterResult(result.Sent)
				o.Filtered(filterRes)

				messagesToSend = result.Failed
				oversized = append(oversized, result.Oversized...)
				invalid = append(invalid, result.Invalid...)
				return err
			}

			err, _ := handleWrite(cfg, writeFiltered, nil)
			if err != nil {
				// There's no retry config for filtered at present, just return error and crash.
				return err
			}
		}

		// Send oversized message buffer
		if len(oversized) > 0 {
			messagesToSend = oversized
			writeOversized := func() error {
				result, err := ft.WriteOversized(t.MaximumAllowedMessageSizeBytes(), messagesToSend)
				if len(result.Oversized) != 0 || len(result.Invalid) != 0 {
					log.Fatal("Oversized message transformation resulted in new oversized / invalid messages")
				}

				o.TargetWriteOversized(result)
				messagesToSend = result.Failed
				return err
			}

			err, _ := handleWrite(cfg, writeOversized, nil)
			// Failure here should always be handled as an exception.
			if err != nil {
				return err
			}
		}

		// Send invalid message buffer
		if len(invalid) > 0 {
			messagesToSend = invalid
			writeInvalid := func() error {
				result, err := ft.WriteInvalid(messagesToSend)
				if len(result.Oversized) != 0 || len(result.Invalid) != 0 {
					log.Fatal("Invalid message transformation resulted in new invalid / oversized messages")
				}

				o.TargetWriteInvalid(result)
				messagesToSend = result.Failed
				return err
			}

			err, _ := handleWrite(cfg, writeInvalid, nil)
			// Failure here should always be handled as an exception.
			if err != nil {
				return err
			}
		}
		return nil
	}
}

// Wrap each target write operation with 2 kinds of retries:
// - setup errors: long delay, unlimited attempts, unhealthy state + alerts
// - transient errors: short delay, limited attempts
// If it's setup/transient error is decided based on a response returned by the target.
func handleWrite(cfg *config.Config, write func() error, alertChan chan error) (err error, sendToInvalid bool) {
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
		retry.Delay(time.Duration(cfg.Data.Retry.Setup.Delay)*time.Millisecond),
		retry.Attempts(uint(cfg.Data.Retry.Setup.MaxAttempts)),
		retry.LastErrorOnly(true),
	)

	// If after retries we still have setup error
	// there is no reason to retry it further, so error early
	if _, isSetup := err.(models.SetupWriteError); isSetup {

		return err, cfg.Data.Retry.Setup.InvalidAfterMax
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
		time.Sleep(time.Duration(cfg.Data.Retry.Throttle.Delay) * time.Millisecond)

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
			retry.Delay(time.Duration(cfg.Data.Retry.Throttle.Delay)*time.Millisecond),
			retry.Attempts(uint(cfg.Data.Retry.Throttle.MaxAttempts)),
			retry.LastErrorOnly(true),
		)
	}

	// If after retries we still have throttle error
	// there is no reason to retry it further, so error early
	if _, isThrottle := err.(models.ThrottleWriteError); isThrottle {

		return err, cfg.Data.Retry.Throttle.InvalidAfterMax
	}

	if err == nil {
		return err, false
	}

	// If no throttle, then handle as transient.
	log.Warnf("Transient target write error. Starting retrying. error: %s\n", err)
	// We already had at least 1 attempt from above 'throttle' retrying section,
	// so before we start transient retrying we need to add 'manual' initial delay.
	time.Sleep(time.Duration(cfg.Data.Retry.Transient.Delay) * time.Millisecond)

	onTransientError := retry.OnRetry(func(retry uint, err error) {
		log.Warnf("Retry failed with transient error. Retry counter: %d, error: %s\n", retry+1, err)
	})

	err = retry.Do(
		write,
		onTransientError,
		// * 2 because we have initial sleep above
		retry.Delay(time.Duration(cfg.Data.Retry.Transient.Delay*2)*time.Millisecond),
		retry.Attempts(uint(cfg.Data.Retry.Transient.MaxAttempts)),
		retry.LastErrorOnly(true),
	)

	return err, cfg.Data.Retry.Transient.InvalidAfterMax
}

// exitWithError will ensure we log the error and leave time for Sentry to flush
func exitWithError(err error, flushSentry bool) {
	log.WithFields(log.Fields{"error": err}).Error(err)
	if flushSentry {
		sentry.Flush(2 * time.Second)
	}
	os.Exit(1)
}

func copyOriginalData(messages []*models.Message) {
	// To preserve original data (which may be needed downstream) we copy data provided by source before we run any transformations
	for _, msg := range messages {
		buffer := make([]byte, len(msg.Data))
		copy(buffer, msg.Data)
		msg.OriginalData = buffer
	}
}
