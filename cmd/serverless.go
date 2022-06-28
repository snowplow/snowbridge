// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package cmd

import (
	"time"

	"github.com/getsentry/sentry-go"
	log "github.com/sirupsen/logrus"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
)

// ServerlessRequestHandler is a common function for all
// serverless implementations to leverage
func ServerlessRequestHandler(messages []*models.Message) error {
	cfg, sentryEnabled, err := Init()
	if err != nil {
		return err
	}
	if sentryEnabled {
		defer sentry.Flush(2 * time.Second)
	}

	// --- Setup structs

	t, err := cfg.GetTarget()
	if err != nil {
		return err
	}
	t.Open()

	ft, err := cfg.GetFailureTarget(AppName, AppVersion)
	if err != nil {
		return err
	}
	ft.Open()

	en, err := cfg.GetEngines()
	if err != nil {
		return err
	}

	tr, err := cfg.GetTransformations(en)
	if err != nil {
		return err
	}

	// --- Process events

	transformed := tr(messages)
	// no error as errors should be returned in the failures array of TransformationResult

	// Ack filtered messages with no further action
	messagesToFilter := transformed.Filtered
	for _, msg := range messagesToFilter {
		if msg.AckFunc != nil {
			msg.AckFunc()
		}
	}

	res, err := t.Write(transformed.Result)
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error(err)
	}

	if len(res.Oversized) > 0 {
		res2, err := ft.WriteOversized(t.MaximumAllowedMessageSizeBytes(), res.Oversized)
		if len(res2.Oversized) != 0 || len(res2.Invalid) != 0 {
			log.Fatal("Oversized message transformation resulted in new oversized / invalid messages")
		}
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Error(err)
		}
	}

	invalid := append(res.Invalid, transformed.Invalid...)

	if len(invalid) > 0 {
		res3, err := ft.WriteInvalid(invalid)
		if len(res3.Oversized) != 0 || len(res3.Invalid) != 0 {
			log.Fatal("Invalid message transformation resulted in new invalid / oversized messages")
		}
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Error(err)
		}
	}

	t.Close()
	ft.Close()
	return err
}
