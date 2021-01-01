// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package internal

import (
	"encoding/json"
	"fmt"
	"github.com/getsentry/sentry-go"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	sentryhook "github.com/snowplow-devops/stream-replicator/third_party/sentryhook"
)

var (
	logLevelsMap = map[string]log.Level{
		"debug":   log.DebugLevel,
		"info":    log.InfoLevel,
		"warning": log.WarnLevel,
		"error":   log.ErrorLevel,
		"fatal":   log.FatalLevel,
		"panic":   log.PanicLevel,
	}
)

// Init contains the core initialization code for each implementation
// and handles:
//
// 1. Loading the Config from the environment
// 2. Configuring Sentry
// 3. Configuring Logrus (+Logrus -> Sentry)
func Init() (*Config, bool, error) {
	cfg, err := NewConfig()
	if err != nil {
		return nil, false, errors.Wrap(err, "Failed to build config")
	}

	// Configure Sentry
	sentryEnabled := cfg.Sentry.Dsn != ""
	if sentryEnabled {
		err := sentry.Init(sentry.ClientOptions{
			Dsn:              cfg.Sentry.Dsn,
			Debug:            cfg.Sentry.Debug,
			AttachStacktrace: true,
		})
		if err != nil {
			return nil, false, errors.Wrap(err, "Failed to build Sentry")
		}

		sentryTagsMap := map[string]string{}
		err = json.Unmarshal([]byte(cfg.Sentry.Tags), &sentryTagsMap)
		if err != nil {
			return nil, false, errors.Wrap(err, "Failed to unmarshall SENTRY_TAGS to map")
		}
		sentry.ConfigureScope(func(scope *sentry.Scope) {
			for key, value := range sentryTagsMap {
				scope.SetTag(key, value)
			}
		})

		log.AddHook(sentryhook.New([]log.Level{log.PanicLevel, log.FatalLevel, log.ErrorLevel}))
	}

	// Configure logging level
	if level, ok := logLevelsMap[cfg.LogLevel]; ok {
		log.SetLevel(level)
	} else {
		return nil, sentryEnabled, fmt.Errorf("Supported log levels are 'debug, info, warning, error, fatal, panic'; provided %s", cfg.LogLevel)
	}

	log.Debugf("Config: %+v", cfg)
	return cfg, sentryEnabled, nil
}