/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package telemetry

import (
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	conf "github.com/snowplow/snowbridge/v3/config"
	gt "github.com/snowplow/snowplow-golang-tracker/v2/tracker"
)

// config holds the configuration for telemetry
type config struct {
	disable            bool
	interval           time.Duration
	method             string
	url                string
	protocol           string
	port               string
	userProvidedID     string
	applicationName    string
	applicationVersion string
	appGeneratedID     string
}

func newTelemetryWithConfig(cfg *conf.Config) *config {
	return &config{
		disable:            cfg.Data.DisableTelemetry,
		interval:           interval,
		method:             method,
		protocol:           protocol,
		url:                url,
		port:               port,
		userProvidedID:     cfg.Data.UserProvidedID,
		applicationName:    applicationName,
		applicationVersion: applicationVersion,
		appGeneratedID:     uuid.New().String(),
	}
}

func initTelemetry(telemetry *config) func() {
	storage := gt.InitStorageMemory()
	emitter := gt.InitEmitter(
		gt.RequireCollectorUri(fmt.Sprintf(`%s:%s`, telemetry.url, telemetry.port)),
		gt.OptionRequestType(telemetry.method),
		gt.OptionProtocol(telemetry.protocol),
		gt.OptionCallback(func(goodResults []gt.CallbackResult, badResults []gt.CallbackResult) {
			for _, goodResult := range goodResults {
				if goodResult.Status != http.StatusOK {
					log.WithFields(log.Fields{
						"error_code": goodResult.Status,
					}).Debugf("Error sending telemetry event")
					return
				}
			}
			for _, badResult := range badResults {
				if badResult.Status != http.StatusOK {
					log.WithFields(log.Fields{
						"error_code": badResult.Status,
					}).Debugf("Error sending telemetry event")
					return
				}
			}
			log.Info(`Telemetry event sent successfully`)
		}),
		gt.OptionStorage(storage),
	)

	tracker := gt.InitTracker(
		gt.RequireEmitter(emitter),
		gt.OptionNamespace("telemetry"),
		gt.OptionAppId(telemetry.applicationName),
	)

	ticker := time.NewTicker(telemetry.interval)

	stop := make(chan struct{})

	go func() {
		makeAndTrackHeartbeat(telemetry, tracker)
		for {
			select {
			case <-ticker.C:
				makeAndTrackHeartbeat(telemetry, tracker)
			case <-stop:
				return
			}

		}
	}()

	return func() {
		close(stop)
	}
}

func makeAndTrackHeartbeat(telemetry *config, tracker *gt.Tracker) {
	event := makeHeartbeatEvent(*telemetry)

	tracker.TrackSelfDescribingEvent(gt.SelfDescribingEvent{
		Event:         event,
		Timestamp:     nil,
		EventId:       nil,
		TrueTimestamp: nil,
		Contexts:      nil,
		Subject:       nil,
	})
}

// InitTelemetryWithCollector initialises telemetry
func InitTelemetryWithCollector(cfg *conf.Config) func() {
	telemetry := newTelemetryWithConfig(cfg)
	if telemetry.disable {
		return func() {}
	}
	return initTelemetry(telemetry)
}

func makeHeartbeatEvent(service config) *gt.SelfDescribingJson {
	payload := gt.InitPayload()

	payload.Add(`userProvidedId`, &service.userProvidedID)
	payload.Add(`applicationName`, &service.applicationName)
	payload.Add(`applicationVersion`, &service.applicationVersion)
	payload.Add(`appGeneratedId`, &service.appGeneratedID)

	selfDescJSON := gt.InitSelfDescribingJson(
		`iglu:com.snowplowanalytics.oss/oss_context/jsonschema/1-0-1`, payload.Get())
	return selfDescJSON
}
