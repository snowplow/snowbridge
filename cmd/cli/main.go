// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package main

import (
	"github.com/getsentry/sentry-go"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/snowplow-devops/stream-replicator/internal"
	"github.com/snowplow-devops/stream-replicator/internal/app"
	"github.com/snowplow-devops/stream-replicator/internal/failure/failureiface"
	"github.com/snowplow-devops/stream-replicator/internal/models"
	"github.com/snowplow-devops/stream-replicator/internal/observer"
	"github.com/snowplow-devops/stream-replicator/internal/source/sourceiface"
	"github.com/snowplow-devops/stream-replicator/internal/target/targetiface"
	"github.com/snowplow-devops/stream-replicator/pkg/retry"
)

const (
	appVersion   = app.Version
	appName      = app.Name
	appUsage     = "Replicates data streams to supported targets"
	appCopyright = "(c) 2020 Snowplow Analytics, LTD"
)

func main() {
	// Init must be run at the top of the stack so that its context is available
	// after app.Action() returns
	cfg, sentryEnabled, err := internal.Init()
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
	app.Flags = []cli.Flag{}
	app.Action = func(c *cli.Context) error {
		source, err := cfg.GetSource()
		if err != nil {
			return err
		}

		t, err := cfg.GetTarget()
		if err != nil {
			return err
		}
		t.Open()

		ft, err := cfg.GetFailureTarget()
		if err != nil {
			return err
		}
		ft.Open()

		o, err := cfg.GetObserver()
		if err != nil {
			return err
		}
		o.Start()

		// Handle SIGTERM
		sig := make(chan os.Signal)
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
			case <-time.After(5 * time.Second):
				log.Error("source.Stop() took more than 5 seconds, forcing shutdown ...")

				t.Close()
				ft.Close()
				o.Stop()

				os.Exit(1)
			}
		}()

		// Callback functions for the source to leverage when writing data
		sf := sourceiface.SourceFunctions{
			WriteToTarget: sourceWriteFunc(t, ft, o),
		}

		// Read is a long running process and will only return when the source
		// is exhausted or if an error occurs
		err = source.Read(&sf)
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
func sourceWriteFunc(t targetiface.Target, ft failureiface.Failure, o *observer.Observer) func(messages []*models.Message) error {
	return func(messages []*models.Message) error {
		res, err := retry.ExponentialWithInterface(5, time.Second, "target.Write", func() (interface{}, error) {
			res, err := t.Write(messages)
			o.TargetWrite(res)
			return res, err
		})
		if err != nil {
			return err
		}
		resCast := res.(*models.TargetWriteResult)

		return retry.Exponential(5, time.Second, "failureTarget.Write", func() error {
			if len(resCast.Oversized) > 0 {
				res, err := ft.WriteOversized(t.MaximumAllowedMessageSizeBytes(), resCast.Oversized)

				// NOTE: This should never happen but we check for it anyway to avoid
				//       unack'able messages ever occurring
				if len(res.Oversized) != 0 {
					log.Fatal("Oversized message transformation resulted in new oversized messages")
				}

				o.TargetWriteOversized(res)
				return err
			}
			return nil
		})
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
