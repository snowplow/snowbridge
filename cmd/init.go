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

package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/getsentry/sentry-go"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	sentryhook "github.com/snowplow-devops/go-sentryhook"

	config "github.com/snowplow/snowbridge/config"
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
func Init() (*config.Config, bool, error) {
	cfg, err := config.NewConfig()
	if err != nil {
		return nil, false, errors.Wrap(err, "Failed to build config")
	}

	// Configure Sentry
	sentryEnabled := cfg.Data.Sentry.Dsn != ""
	if sentryEnabled {
		err := sentry.Init(sentry.ClientOptions{
			Dsn:              cfg.Data.Sentry.Dsn,
			Debug:            cfg.Data.Sentry.Debug,
			AttachStacktrace: true,
		})
		if err != nil {
			return nil, false, errors.Wrap(err, "Failed to build Sentry")
		}

		sentryTagsMap := map[string]string{}
		err = json.Unmarshal([]byte(cfg.Data.Sentry.Tags), &sentryTagsMap)
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
	if level, ok := logLevelsMap[cfg.Data.LogLevel]; ok {
		log.SetLevel(level)
	} else {
		return nil, sentryEnabled, fmt.Errorf("Supported log levels are 'debug, info, warning, error, fatal, panic'; provided %s", cfg.Data.LogLevel)
	}

	log.Debugf("Config: %+v", cfg)
	return cfg, sentryEnabled, nil
}
