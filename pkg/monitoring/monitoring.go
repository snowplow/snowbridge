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
	"fmt"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
)

type MonitoringEvent struct {
	Schema string         `json:"schema"`
	Data   MonitoringData `json:"data"`
}

type MonitoringData struct {
	AppName    string            `json:"appName"`
	AppVersion string            `json:"appVersion"`
	Tags       map[string]string `json:"tags"`

	Message string `json:"message,omitempty"`
}

// MonitoringSender describes the interface for how to send heartbeat & alert events
type MonitoringSender interface {
	Do(req *http.Request) (*http.Response, error)
}

// Monitoring holds a new client and supporting data for sending heartbeat & alert events
type Monitoring struct {
	appName           string
	appVersion        string
	client            MonitoringSender
	endpoint          string
	tags              map[string]string
	heartbeatInterval time.Duration
	alertChan         chan error
	log               *logrus.Entry

	exitSignal chan struct{}
}

func NewMonitoring(appName, appVersion string, client MonitoringSender, endpoint string, tags map[string]string, heartbeatInterval time.Duration, alertChan chan error) *Monitoring {
	fmt.Printf("[NewMonitoring] with interval: %s\n", heartbeatInterval)

	return &Monitoring{
		appName:           appName,
		appVersion:        appVersion,
		client:            client,
		endpoint:          endpoint,
		tags:              tags,
		heartbeatInterval: heartbeatInterval,
		log:               logrus.WithFields(logrus.Fields{"name": "Monitoring"}),
		alertChan:         alertChan,
		exitSignal:        make(chan struct{}),
	}
}

func (m *Monitoring) Start() {
	ticker := time.NewTicker(m.heartbeatInterval)

	go func() {

	OuterLoop:
		for {
			select {
			case <-ticker.C:
				m.log.Info("heartbeat ticked")
				if m.client != nil {
					m.log.Info("Sending heartbeat")
					event := MonitoringEvent{
						Schema: "iglu:com.snowplowanalytics.monitoring.loader/heartbeat/jsonschema/1-0-0",
						Data: MonitoringData{
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
				if m.client != nil {
					m.log.Info("Sending alert")
					event := MonitoringEvent{
						Schema: "iglu:com.snowplowanalytics.monitoring.loader/alert/jsonschema/1-0-0",
						Data: MonitoringData{
							AppName:    m.appName,
							AppVersion: m.appVersion,
							Tags:       m.tags,
							Message:    err.Error(),
						},
					}
					req, err := m.prepareRequest(event)
					if err != nil {
						m.log.Warnf("failed to prepare heartbeat event request: %s", err)
						continue
					}

					_, err = m.client.Do(req)
					if err != nil {
						m.log.Warnf("failed to send alert event: %s", err)
					}

					// Once alert has been successfully sent,
					// we shouldn't attempt to send anything else (nor alert, nor heartbeat)
					m.client = nil
				}
			case <-m.exitSignal:
				m.log.Info("Monitoring is shutting down")
				break OuterLoop
			}
		}
	}()
}

func (m *Monitoring) Stop() {
	m.exitSignal <- struct{}{}
}

func (m *Monitoring) prepareRequest(event MonitoringEvent) (*http.Request, error) {
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
