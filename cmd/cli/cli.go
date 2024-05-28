/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package cli

import (
	"bytes"
	"encoding/json"
	"html/template"
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

// RunCli runs the app
func RunCli(supportedSources []config.ConfigurationPair, supportedTransformations []config.ConfigurationPair) {
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
				http.ListenAndServe("localhost:8080", nil)
			}()
		}

		s, err := sourceconfig.GetSource(cfg, supportedSources)
		if err != nil {
			return err
		}

		tr, err := transformconfig.GetTransformationsRefactored(cfg, supportedTransformations)
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
				if err != nil {
					log.Debugf(`error deleting tmp directory: %v`, err)
				}
			case <-time.After(5 * time.Second):
				log.Error("source.Stop() took more than 5 seconds, forcing shutdown ...")

				t.Close()
				ft.Close()
				o.Stop()
				stopTelemetry()

				if err != nil {
					log.Debugf(`error deleting tmp directory: %v`, err)
				}

				os.Exit(1)
			}
		}()

		// The channel will need a buffer limit. This should probably be a configuration, and we should think about what to set it as & what effect this may have on latency
		batchingChannel := make(chan *models.Message, 10000)

		// Spawn a function to consume the channel, via a goroutine:
		go func() {
			// This interval would be configurable
			ticker := time.NewTicker(1 * time.Second)
			currentBatch := []*models.Message{}
			for {
				select {
				case msg := <-batchingChannel:
					// Append message to current batch
					currentBatch = append(currentBatch, msg)
					// Logic can go here to immediately send when the batch is of a certain size of data, with some extra effort
					// For now let's mimic that by using number of events
					// (We could have both be configurable!)
					if len(currentBatch) >= 100 {
						// Process the batch
						go batchTransformAndWriteData([]*models.TargetBatch{{
							OriginalMessages: currentBatch,
						}}, t, ft, o)
						// Clear the placeholder for the batch.
						currentBatch = []*models.Message{}
						// Ofc tests should be written to ensure threadsafety here.
						// I don't believe we can reach a point where the next loop executes before this is finished, however - since both happen in this same goroutine
					}
				case <-ticker.C:
					// Every tick, process a batch.
					// If we like, we could get custom and restart tickers when the other case gets executed.
					go batchTransformAndWriteData([]*models.TargetBatch{{
						OriginalMessages: currentBatch,
					}}, t, ft, o)
					currentBatch = []*models.Message{}

				}
			}
		}()

		// Callback functions for the source to leverage when writing data
		sf := sourceiface.SourceFunctions{
			WriteToTarget: sourceReadAndTransformFunc(tr, ft, o, batchingChannel),
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

func batchTransformAndWriteData(targetBatches []*models.TargetBatch, t targetiface.Target, ft failureiface.Failure, o *observer.Observer) error {
	messageBatches := BatchTransformationFunction(targetBatches)

	// While we're refactoring, retry may be best suited to living in the write function too.
	res, err := retry.ExponentialWithInterface(5, time.Second, "target.Write", func() (interface{}, error) {
		res, err := t.Write(messageBatches)

		o.TargetWrite(res)
		// messagesToSend = res.Failed
		// ^^ This bit needs to be looked at
		return res, err
	})
	if err != nil {
		return err
	}
	resCast := res.(*models.TargetWriteResult)

	// Send oversized message buffer
	messagesToSend := resCast.Oversized
	if len(messagesToSend) > 0 {
		err2 := retry.Exponential(5, time.Second, "failureTarget.WriteOversized", func() error {
			res, err := ft.WriteOversized(t.MaximumAllowedMessageSizeBytes(), messagesToSend)
			if err != nil {
				return err
			}
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
	messagesToSend = resCast.Invalid
	if len(messagesToSend) > 0 {
		err3 := retry.Exponential(5, time.Second, "failureTarget.WriteInvalid", func() error {
			res, err := ft.WriteInvalid(messagesToSend)
			if err != nil {
				return err
			}
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

// This would replace the sourceWrite function, and the batch transformation and target would read from the supplied channel
func sourceReadAndTransformFunc(tr transform.TransformationApplyFunctionRefactored, ft failureiface.Failure, o *observer.Observer, c chan *models.Message) func(messages []*models.Message) error {
	return func(messages []*models.Message) error {

		// Successful transforms are immediately fed to the channel, we no longer process them in this function.
		// Same with acking filtered messages. We can do that immediately.
		// For now, we can continue to deal with failures here - but in a real implementation perhaps those should be handled in a similar flow.
		transformResult := tr(messages, c)

		// observer stuff can still go here
		filterRes := models.NewFilterResult(transformResult.Filtered)
		o.Filtered(filterRes)

		// Deal with transformed invalids -
		// TODO: This pattern should probably change in a full refactor, and perhaps use a separate channel too. :thinking_face:
		// It def should, then we can have all sources of invalids pop their data into a channel (as we encounter them), and have one thing read from it and deal with it.
		messagesToSend := transformResult.Invalid
		if len(messagesToSend) > 0 {
			err3 := retry.Exponential(5, time.Second, "failureTarget.WriteInvalid", func() error {
				res, err := ft.WriteInvalid(messagesToSend)
				if err != nil {
					return err
				}
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

// BatchTransformationFunction would live elsewhere and be composable like the other transoformation functions
func BatchTransformationFunction(batch []*models.TargetBatch) []*models.TargetBatch {

	// imaine this is composable like transformaion functions, and does something :D

	// The templater would fit here along the following lines:
	const templ = `{
		attributes: [ {{$first_1 := true}}
		  {{range .}}{{if $first_1}}{{$first_1 = false}}{{else}},{{end}}
		  {{printf "%s" .attribute_data}}{{end}}
		  ],
		events: [ {{$first_2 := true}}
		  {{range .}}{{if $first_2}}{{$first_2 = false}}{{else}},{{end}}
		  {{printf "%s" .event_data}}{{end}}
		  ]
	  }`

	for _, b := range batch {
		formatted := []map[string]json.RawMessage{}
		for _, msg := range b.OriginalMessages {
			// Use json.RawMessage to ensure templating format works (real implementation has a problem to figure out here)
			var asMap map[string]json.RawMessage

			if err := json.Unmarshal(msg.Data, &asMap); err != nil {
				panic(err)
			}

			formatted = append(formatted, asMap)
		}
		var buf bytes.Buffer

		t := template.Must(template.New("example").Parse(templ))
		t.Execute(&buf, formatted)

		// Assign the templated request to the HTTPRequestBody field
		b.HTTPRequestBody = buf.Bytes()

	}

	return batch
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

		messageBatches := []*models.TargetBatch{{
			OriginalMessages: messagesToSend,
			HTTPRequestBody:  nil}}

		messageBatches = BatchTransformationFunction(messageBatches)

		res, err := retry.ExponentialWithInterface(5, time.Second, "target.Write", func() (interface{}, error) {
			res, err := t.Write(messageBatches)

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
				if err != nil {
					return err
				}
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
				if err != nil {
					return err
				}
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
