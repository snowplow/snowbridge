// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package main

import (
	"bufio"
	log "github.com/sirupsen/logrus"
	"github.com/twinj/uuid"
	"github.com/urfave/cli"
	"os"
	"time"

	core "github.com/snowplow-devops/stream-replicator/core"
)

func main() {
	app := cli.NewApp()
	app.Name = "stream-replicator"
	app.Copyright = "(c) 2020 Snowplow Analytics, LTD"
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
			log.Panicf(err.Error())
		}

		t, err := cfg.GetTarget()
		if err != nil {
			log.Panicf("FATAL: config.GetTarget: %s", err.Error())
		}

		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			events := make([]*core.Event, 1)
			events[0] = &core.Event{
				Data:         []byte(scanner.Text()),
				PartitionKey: uuid.NewV4().String(),
			}

			err := t.Write(events)
			if err != nil {
				log.Error(err)
			}
		}

		if scanner.Err() != nil {
			log.Error(scanner.Err())
			return scanner.Err()
		}

		return nil
	}

	app.Run(os.Args)
}
