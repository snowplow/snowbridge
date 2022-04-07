// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package cli

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/getsentry/sentry-go"
	log "github.com/sirupsen/logrus"
	retry "github.com/snowplow-devops/go-retry"
	"github.com/urfave/cli"

	"net/http"
	// pprof imported for the side effect of registering its HTTP handlers
	_ "net/http/pprof"

	"github.com/snowplow-devops/stream-replicator/cmd"
	"github.com/snowplow-devops/stream-replicator/pkg/common"
	"github.com/snowplow-devops/stream-replicator/pkg/failure/failureiface"
	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow-devops/stream-replicator/pkg/observer"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceconfig"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceiface"
	"github.com/snowplow-devops/stream-replicator/pkg/target/targetiface"
	"github.com/snowplow-devops/stream-replicator/pkg/telemetry"
	"github.com/snowplow-devops/stream-replicator/pkg/transform"
	"github.com/snowplow-devops/stream-replicator/pkg/transform/transformconfig"
)

const (
	appVersion   = cmd.AppVersion
	appName      = cmd.AppName
	appUsage     = "Replicates data streams to supported targets"
	appCopyright = "(c) 2020-2022 Snowplow Analytics, LTD"
)

// RunCli runs the app
func RunCli(supportedSourceConfigPairs []sourceconfig.ConfigPair) {
	cfg, sentryEnabled, err := cmd.Init()
	if err != nil {
		exitWithError(err, sentryEnabled)
	}

	app := cli.NewApp()
	app.Name = appName
	app.Usage = appUsage
	app.Version = appVersion
	app.Copyright = appCopyright
	app.Compiled = time.Now()
	app.Authors = []cli.Author{
		{
			Name:  "Joshua Beemster",
			Email: "tech-ops-team@snowplowanalytics.com",
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
				http.ListenAndServe("localhost:8080", nil)
			}()
		}

		s, err := sourceconfig.GetSource(cfg, supportedSourceConfigPairs)
		if err != nil {
			return err
		}

		tr, err := transformconfig.GetTransformations(cfg)
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
				err := common.DeleteTemporaryDir()
				if err != nil {
					log.Debugf(`error deleting tmp directory: %v`, err)
				}
			case <-time.After(5 * time.Second):
				log.Error("source.Stop() took more than 5 seconds, forcing shutdown ...")

				t.Close()
				ft.Close()
				o.Stop()
				stopTelemetry()

				err := common.DeleteTemporaryDir()
				if err != nil {
					log.Debugf(`error deleting tmp directory: %v`, err)
				}

				os.Exit(1)
			}
		}()

		// Callback functions for the source to leverage when writing data
		sf := sourceiface.SourceFunctions{
			WriteToTarget: sourceWriteFunc(t, ft, tr, o),
		}

		// Read is a long running process and will only return when the source
		// is exhausted or if an error occurs
		err = s.Read(&sf)
		if err != nil {
			return err
		}

		t.Close()
		ft.Close()
		o.Stop()
		return nil
	}

	err1 := app.Run(os.Args)
	if err1 != nil {
		exitWithError(err1, sentryEnabled)
	}

}

// sourceWriteFunc builds the function which wraps the different objects together to handle:
//
// 1. Sending messages to the target
// 2. Observing results
// 3. Sending oversized messages to the failure target
// 4. Observing these results
//
// All with retry logic baked in to remove any of this handling from the implementations
func sourceWriteFunc(t targetiface.Target, ft failureiface.Failure, tr transform.TransformationApplyFunction, o *observer.Observer) func(messages []*models.Message) error {
	return func(messages []*models.Message) error {

		// Apply transformations
		transformed := tr(messages)
		// no error as errors should be returned in the failures array of TransformationResult

		// Ack filtered messages with no further action
		messagesToFilter := transformed.Filtered
		for _, msg := range messagesToFilter {
			if msg.AckFunc != nil {
				msg.AckFunc()
			}
		}
		// Push filter result to observer
		filterRes := models.NewFilterResult(messagesToFilter)
		o.Filtered(filterRes)

		// Send message buffer
		messagesToSend := transformed.Result

		res, err := retry.ExponentialWithInterface(5, time.Second, "target.Write", func() (interface{}, error) {
			res, err := t.Write(messagesToSend)

			o.TargetWrite(res)
			messagesToSend = res.Failed
			return res, err
		})
		if err != nil {
			return err
		}
		resCast := res.(*models.TargetWriteResult)

		// Send oversized message buffer
		messagesToSend = resCast.Oversized
		if len(messagesToSend) > 0 {
			err2 := retry.Exponential(5, time.Second, "failureTarget.WriteOversized", func() error {
				res, err := ft.WriteOversized(t.MaximumAllowedMessageSizeBytes(), messagesToSend)
				if len(res.Oversized) != 0 || len(res.Invalid) != 0 {
					log.Fatal("Oversized message transformation resulted in new oversized / invalid messages")
				}

				o.TargetWriteOversized(res)
				messagesToSend = res.Failed
				return err
			})
			if err2 != nil {
				return err2
			}
		}

		// Send invalid message buffer
		messagesToSend = append(resCast.Invalid, transformed.Invalid...)
		if len(messagesToSend) > 0 {
			err3 := retry.Exponential(5, time.Second, "failureTarget.WriteInvalid", func() error {
				res, err := ft.WriteInvalid(messagesToSend)
				if len(res.Oversized) != 0 || len(res.Invalid) != 0 {
					log.Fatal("Invalid message transformation resulted in new invalid / oversized messages")
				}

				o.TargetWriteInvalid(res)
				messagesToSend = res.Failed
				return err
			})
			if err3 != nil {
				return err3
			}
		}

		return nil
	}
}

// exitWithError will ensure we log the error and leave time for Sentry to flush
func exitWithError(err error, flushSentry bool) {
	log.WithFields(log.Fields{"error": err}).Error(err)
	if flushSentry {
		sentry.Flush(2 * time.Second)
	}
	os.Exit(1)
}
