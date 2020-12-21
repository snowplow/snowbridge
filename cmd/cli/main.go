// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package main

import (
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"os"
	"time"

	core "github.com/snowplow-devops/stream-replicator/core"
)

const (
	appVersion   = "0.1.0"
	appName      = "stream-replicator"
	appUsage     = "Replicates data streams to supported targets"
	appCopyright = "(c) 2020 Snowplow Analytics, LTD"
)

func main() {
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
		cfg, err := core.Init()
		if err != nil {
			return err
		}

		source, err := cfg.GetSource()
		if err != nil {
			return err
		}
		target, err := cfg.GetTarget()
		if err != nil {
			return err
		}

		// Callback functions for the source to leverage when writing data
		sf := core.SourceFunctions{
			Write: target.Write,
		}

		// Note: Read is a long running process and will only return when the source
		//       is exhausted or if an error occurs
		err = source.Read(&sf)
		if err != nil {
			return err
		}

		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}
}
