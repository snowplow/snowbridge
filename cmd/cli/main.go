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

	app.Flags = []cli.Flag{
		cli.GenericFlag{
			Name:  "source, s",
			Usage: "Input data from stdin",
			Value: &EnumValue{
				Enum:    []string{"stdin"},
				Default: "stdin",
			},
			EnvVar: "SOURCE",
		},
	}

	app.Action = func(c *cli.Context) error {
		cfg, err := core.Init()
		if err != nil {
			log.Panicf(err.Error())
		}

		s, err1 := cfg.GetSource()
		if err1 != nil {
			log.Panicf("FATAL: config.GetSource: %s", err1.Error())
		}

		t, err2 := cfg.GetTarget()
		if err2 != nil {
			log.Panicf("FATAL: config.GetTarget: %s", err2.Error())
		}

		// TODO: Read + Write should be an infinite loop until SIGTERM is given

		events, err3 := s.Read()
		if err3 != nil {
			log.Error(err3)
			return err3
		}

		err4 := t.Write(events)
		if err4 != nil {
			log.Error(err4)
			return err4
		}

		return nil
	}

	app.Run(os.Args)
}
