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
	"os/signal"
	"syscall"
	"time"

	core "github.com/snowplow-devops/stream-replicator/core"
)

const (
	appVersion   = "0.1.0-rc2"
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

		// Build source & target
		source, err := cfg.GetSource()
		if err != nil {
			return err
		}
		defer source.Stop()

		target, err := cfg.GetTarget()
		if err != nil {
			return err
		}
		defer target.Close()
		target.Open()

		// Build observer which will be used for telemetry
		observer, err := cfg.GetObserver()
		if err != nil {
			return err
		}
		defer observer.Stop()
		observer.Start()

		// Handle SIGTERM
		sig := make(chan os.Signal)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM, os.Kill)
		go func() {
			<-sig
			log.Warn("SIGTERM called, cleaning up and closing application ...")

			source.Stop()
			target.Close()
			observer.Stop()
		}()

		// Extend target.Write() to push metrics to the observer
		writeFunc := func(messages []*core.Message) error {
			res, err := target.Write(messages)
			observer.TargetWrite(res)
			return err
		}

		// Callback functions for the source to leverage when writing data
		sf := core.SourceFunctions{
			WriteToTarget: writeFunc,
		}

		// Read is a long running process and will only return when the source
		// is exhausted or if an error occurs
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
