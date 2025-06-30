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
	"github.com/snowplow/snowbridge/cmd"
	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/failure/failureiface"
	"github.com/snowplow/snowbridge/pkg/models"
	"github.com/snowplow/snowbridge/pkg/observer"
	"github.com/snowplow/snowbridge/pkg/source/sourceconfig"
	"github.com/snowplow/snowbridge/pkg/source/sourceiface"
	"github.com/snowplow/snowbridge/pkg/target/targetiface"
	"github.com/snowplow/snowbridge/pkg/telemetry"
	"github.com/snowplow/snowbridge/pkg/transform"
	"github.com/snowplow/snowbridge/pkg/transform/transformconfig"
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
	// First thing is to spin up monitoring, so we can start alerting as soon as possible
	monitoring, alertChan, err := cfg.GetMonitoring(cmd.AppName, cmd.AppVersion)
	if err != nil {
		return err
	}
	if monitoring != nil {
		defer monitoring.Stop()
		monitoring.Start()
	}

	s, err := sourceconfig.GetSource(cfg, supportedSources)
	if err != nil {
		return err
	}

	tr, err := transformconfig.GetTransformations(cfg, supportedTransformations)
	if err != nil {
		return err
	}

	t, err := cfg.GetTarget()
	if err != nil {
		return err
	}
	t.Open()

	ft, err := cfg.GetFailureTarget(cmd.AppName, cmd.AppVersion)
	if err != nil {
		return err
	}
	ft.Open()

	filter, err := cfg.GetFilterTarget()
	if err != nil {
		return err
	}
	filter.Open()

	tags, err := cfg.GetTags()
	if err != nil {
		return err
	}
	o, err := cfg.GetObserver(tags)
	if err != nil {
		return err
	}
	o.Start()

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
			s.Stop()
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

			t.Close()
			ft.Close()
			filter.Close()
			o.Stop()
			stopTelemetry()

			if err != nil {
				log.Debugf(`error deleting tmp directory: %v`, err)
			}

			os.Exit(1)
		}
	}()

	// Callback functions for the source to leverage when writing data
	sf := sourceiface.SourceFunctions{
		WriteToTarget: sourceWriteFunc(t, ft, filter, tr, o, cfg, alertChan),
	}

	// Read is a long running process and will only return when the source
	// is exhausted or if an error occurs
	err = s.Read(&sf)
	if err != nil {
		return err
	}

	t.Close()
	ft.Close()
	filter.Close()
	o.Stop()
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

			err := handleWrite(cfg, writeTransformed, alertChan)
			if err != nil {
				return err
			}
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

			err := handleWrite(cfg, writeFiltered, nil)
			if err != nil {
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

			err := handleWrite(cfg, writeOversized, nil)

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

			err := handleWrite(cfg, writeInvalid, nil)

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
func handleWrite(cfg *config.Config, write func() error, alertChan chan error) error {
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
	err := retry.Do(
		write,
		retryOnlySetupErrors,
		onSetupError,
		retry.Delay(time.Duration(cfg.Data.Retry.Setup.Delay)*time.Millisecond),
		// for now let's limit attempts to 5 for setup errors, because we don't have health check which would allow app to be killed externally
		retry.Attempts(5),
		retry.LastErrorOnly(true),
		// enable when health probe is implemented
		// retry.Attempts(0), //unlimited
	)

	// If after retries we still have setup error, there is no reason to retry it further as transient
	// So error early
	if _, isSetup := err.(models.SetupWriteError); isSetup {
		return err
	}

	// Now, `err` is either nil or no longer setup-related
	// Thus we should reset monitoring to re-enable heartbeats
	if alertChan != nil && setupErrored {
		alertChan <- nil
	}

	if err == nil {
		return err
	}

	// If no setup, then handle as transient.
	log.Warnf("Transient target write error. Starting retrying. error: %s\n", err)

	// We already had at least 1 attempt from above 'setup' retrying section, so before we start transient retrying we need add 'manual' initial delay.
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
	return err
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
