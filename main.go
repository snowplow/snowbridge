// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package main

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/getsentry/sentry-go"
	"github.com/makasim/sentryhook"
	log "github.com/sirupsen/logrus"
	"strings"
	"time"
)

const (
	appVersion = "0.1.0-rc1"
	appName    = "stream-replicator"
)

// HandleRequest processes the Kinesis event and forwards it onto another stream
func HandleRequest(ctx context.Context, event events.KinesisEvent) {
	logLevels := map[string]log.Level{
		"debug":   log.DebugLevel,
		"info":    log.InfoLevel,
		"warning": log.WarnLevel,
		"error":   log.ErrorLevel,
		"fatal":   log.FatalLevel,
		"panic":   log.PanicLevel,
	}
	logLevelKeys := getLogLevelKeys(logLevels)

	cfg := NewConfig()

	// Configure Sentry
	if cfg.Sentry.Dsn != "" {
		err := sentry.Init(sentry.ClientOptions{
			Dsn:   cfg.Sentry.Dsn,
			Debug: cfg.Sentry.Debug,
		})
		if err != nil {
			log.Panicf("FATAL: sentry.Init: %s", err.Error())
		}
		defer sentry.Flush(2 * time.Second)

		sentryTagsMap := map[string]string{}
		err = json.Unmarshal([]byte(cfg.Sentry.Tags), &sentryTagsMap)
		if err != nil {
			log.Panicf("FATAL: Failed to unmarshall SENTRY_TAGS to map: %s", err.Error())
		}
		sentry.ConfigureScope(func(scope *sentry.Scope) {
			for key, value := range sentryTagsMap {
				scope.SetTag(key, value)
			}
		})

		log.AddHook(sentryhook.New([]log.Level{log.PanicLevel, log.FatalLevel, log.ErrorLevel}))
	}

	// Configure logging level
	if level, ok := logLevels[cfg.LogLevel]; ok {
		log.SetLevel(level)
	} else {
		log.Panicf("Supported log levels are %s, provided %s",
			strings.Join(logLevelKeys, ","), cfg.LogLevel)
	}

	// Build target client
	t, err := cfg.GetTarget()
	if err != nil {
		log.Panicf("FATAL: config.GetTarget: %s", err.Error())
	}
	t.Write(event)
}

func main() {
	lambda.Start(HandleRequest)
}

// --- HELPERS

func getLogLevelKeys(logLevels map[string]log.Level) []string {
	keys := make([]string, 0, len(logLevels))
	for k := range logLevels {
		keys = append(keys, k)
	}
	return keys
}
