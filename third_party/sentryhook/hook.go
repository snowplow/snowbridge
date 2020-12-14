// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

// Ideas from: https://github.com/makasim/sentryhook

package sentryhook

import (
	"github.com/getsentry/sentry-go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"reflect"
	"strings"
	"time"
)

var (
	logToSentryMap = map[logrus.Level]sentry.Level{
		logrus.TraceLevel: sentry.LevelDebug,
		logrus.DebugLevel: sentry.LevelDebug,
		logrus.InfoLevel:  sentry.LevelInfo,
		logrus.WarnLevel:  sentry.LevelWarning,
		logrus.ErrorLevel: sentry.LevelError,
		logrus.FatalLevel: sentry.LevelFatal,
		logrus.PanicLevel: sentry.LevelFatal,
	}
)

// Hook contains the structure for a Logrus hook
type Hook struct {
	hub    *sentry.Hub
	levels []logrus.Level
}

// New returns a new hook for use by Logrus
func New(levels []logrus.Level) Hook {
	return Hook{
		levels: levels,
		hub:    sentry.CurrentHub(),
	}
}

// Levels returns the levels that this hook fires on
func (hook Hook) Levels() []logrus.Level {
	return hook.levels
}

// Fire sends an event to Sentry
func (hook Hook) Fire(entry *logrus.Entry) error {
	event := sentry.NewEvent()

	event.Level = logToSentryMap[entry.Level]
	event.Message = entry.Message

	for k, v := range entry.Data {
		if k != logrus.ErrorKey {
			event.Extra[k] = v
		}
	}

	if err, ok := entry.Data[logrus.ErrorKey].(error); ok {
		// Use the final message as the error "type"
		lastMsg := strings.Split(err.Error(), ":")[0]

		// Extract the cause to set the base error type
		cause := errors.Cause(err)

		exception := sentry.Exception{
			Type:  lastMsg,
			Value: reflect.TypeOf(cause).String(),
		}

		if hook.hub.Client().Options().AttachStacktrace {
			exception.Stacktrace = sentry.ExtractStacktrace(err)
		}

		event.Exception = []sentry.Exception{exception}
	}

	hook.hub.CaptureEvent(event)

	if entry.Level == logrus.FatalLevel {
		hook.hub.Flush(2 * time.Second)
	}

	return nil
}
