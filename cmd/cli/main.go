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
	"github.com/snowplow-devops/stream-replicator/internal/models"
	"github.com/snowplow-devops/stream-replicator/internal/source/sourceiface"
	"github.com/snowplow-devops/stream-replicator/pkg/retry"
)

const (
	appVersion   = "0.2.0"
	appName      = "stream-replicator"
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

		target, err := cfg.GetTarget()
		if err != nil {
			return err
		}
		target.Open()

		observer, err := cfg.GetObserver()
		if err != nil {
			return err
		}
		observer.Start()

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

				target.Close()
				observer.Stop()

				os.Exit(1)
			}
		}()

		// Extend target.Write() to push metrics to the observer
		writeFunc := func(messages []*models.Message) error {
			return retry.Retry(5, time.Second, "target.Write", func() error {
				res, err := target.Write(messages)
				observer.TargetWrite(res)
				return err
			})
		}

		// Callback functions for the source to leverage when writing data
		sf := sourceiface.SourceFunctions{
			WriteToTarget: writeFunc,
		}

		// Read is a long running process and will only return when the source
		// is exhausted or if an error occurs
		err = source.Read(&sf)
		if err != nil {
			return err
		}

		target.Close()
		observer.Stop()
		return nil
	}

	err1 := app.Run(os.Args)
	if err1 != nil {
		exitWithError(err1, sentryEnabled)
	}
}

func exitWithError(err error, flushSentry bool) {
	log.WithFields(log.Fields{"error": err}).Error(err)
	if flushSentry {
		sentry.Flush(2 * time.Second)
	}
	os.Exit(1)
}
