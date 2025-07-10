/**
 * Copyright (c) 2025-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package monitoring

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
)

type WebhookEvent struct {
	Schema string      `json:"schema"`
	Data   WebhookData `json:"data"`
}

type WebhookData struct {
	AppName    string            `json:"appName"`
	AppVersion string            `json:"appVersion"`
	Tags       map[string]string `json:"tags"`

	Message string `json:"message,omitempty"`
}

// WebhookSender describes the interface for how to send heartbeat & alert events
type WebhookSender interface {
	Do(req *http.Request) (*http.Response, error)
}

// WebhookMonitoring holds a new client and supporting data for sending heartbeat & alert events
type WebhookMonitoring struct {
	appName           string
	appVersion        string
	client            WebhookSender
	endpoint          string
	tags              map[string]string
	heartbeatInterval time.Duration
	alertChan         chan error
	log               *logrus.Entry

	exitSignal chan struct{}

	isHealthy bool
}

func NewWebhookMonitoring(appName, appVersion string, client WebhookSender, endpoint string, tags map[string]string, heartbeatInterval time.Duration, alertChan chan error) *WebhookMonitoring {
	return &WebhookMonitoring{
		appName:           appName,
		appVersion:        appVersion,
		client:            client,
		isHealthy:         true,
		endpoint:          endpoint,
		tags:              tags,
		heartbeatInterval: heartbeatInterval,
		log:               logrus.WithFields(logrus.Fields{"name": "WebhookMonitoring"}),
		alertChan:         alertChan,
		exitSignal:        make(chan struct{}),
	}
}

func (m *WebhookMonitoring) Start() {
	ticker := time.NewTicker(m.heartbeatInterval)

	go func() {

	OuterLoop:
		for {
			select {
			case <-ticker.C:
				if m.isHealthy {
					m.log.Info("Sending heartbeat")
					event := WebhookEvent{
						Schema: "iglu:com.snowplowanalytics.monitoring.loader/heartbeat/jsonschema/1-0-0",
						Data: WebhookData{
							AppName:    m.appName,
							AppVersion: m.appVersion,
							Tags:       m.tags,
						},
					}
					req, err := m.prepareRequest(event)
					if err != nil {
						m.log.Warnf("failed to prepare heartbeat event request: %s", err)
						continue
					}

					_, err = m.client.Do(req)
					if err != nil {
						m.log.Warnf("failed to send heartbeat event: %s", err)
					}
				}
			case err := <-m.alertChan:
				if m.isHealthy && err != nil {
					m.log.Info("Sending alert")
					event := WebhookEvent{
						Schema: "iglu:com.snowplowanalytics.monitoring.loader/alert/jsonschema/1-0-0",
						Data: WebhookData{
							AppName:    m.appName,
							AppVersion: m.appVersion,
							Tags:       m.tags,
							Message:    err.Error(),
						},
					}
					req, err := m.prepareRequest(event)
					if err != nil {
						m.log.Warnf("failed to prepare alert event request: %s", err)
						continue
					}

					_, err = m.client.Do(req)
					if err != nil {
						m.log.Warnf("failed to send alert event: %s", err)
					}

					// Once alert has been successfully sent,
					// we shouldn't attempt to send anything else (nor alert, nor heartbeat)
					// until setup error is resolved
					m.isHealthy = false
				}

				// If error is nil, it means setup error got resolved
				// and we should resume monitoring
				if err == nil {
					m.isHealthy = true
				}
			case <-m.exitSignal:
				m.log.Info("WebhookMonitoring is shutting down")
				break OuterLoop
			}
		}
	}()
}

func (m *WebhookMonitoring) Stop() {
	m.exitSignal <- struct{}{}
}

func (m *WebhookMonitoring) prepareRequest(event WebhookEvent) (*http.Request, error) {
	header := http.Header{}
	header.Add("Content-Type", "application/json")

	var body bytes.Buffer
	err := json.NewEncoder(&body).Encode(event)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(
		http.MethodPost,
		m.endpoint,
		&body,
	)
	if err != nil {
		return nil, err
	}

	req.Header = header
	return req, nil
}
