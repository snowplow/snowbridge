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

package main

import (
	"net/http"
	"os"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/getsentry/sentry-go"
	"github.com/snowplow/snowbridge/v3/cmd"
	snowbridge_cli "github.com/snowplow/snowbridge/v3/cmd/cli"
	"github.com/snowplow/snowbridge/v3/pkg/transform/transformconfig"
	"github.com/urfave/cli"
)

func main() {
	config, sentryEnabled, err := cmd.Init()
	if err != nil {
		exitWithError(err, sentryEnabled)
	}
	app := cli.NewApp()
	app.Name = cmd.AppName
	app.Usage = cmd.AppUsage
	app.Version = cmd.AppVersion
	app.Copyright = cmd.AppCopyright
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
		return snowbridge_cli.RunApp(config, transformconfig.SupportedTransformations)
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

// exitWithError will ensure we log the error and leave time for Sentry to flush
func exitWithError(err error, flushSentry bool) {
	log.WithFields(log.Fields{"error": err}).Error(err)
	if flushSentry {
		sentry.Flush(2 * time.Second)
	}
	os.Exit(1)
}
