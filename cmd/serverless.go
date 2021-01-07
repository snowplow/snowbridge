// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package cmd

import (
	"github.com/getsentry/sentry-go"
	log "github.com/sirupsen/logrus"
	"time"

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

	ft, err := cfg.GetFailureTarget()
	if err != nil {
		return err
	}
	ft.Open()

	// --- Process events

	res, err := t.Write(messages)
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

	if len(res.Invalid) > 0 {
		res3, err := ft.WriteInvalid(res.Invalid)
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
