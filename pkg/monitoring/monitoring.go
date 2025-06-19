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
	"net/http"
	"net/url"
	"time"

	"github.com/sirupsen/logrus"
)

// MonitoringSender describes the interface for how to send heartbeat & alert events
type MonitoringSender interface {
	Do(req *http.Request) (*http.Response, error)
}

// Monitoring holds a new client and supporting data for sending heartbeat & alert events
type Monitoring struct {
	client            MonitoringSender
	endpoint          string
	tags              map[string]string
	heartbeatInterval time.Duration
	alertChan         chan error
	log               *logrus.Entry

	exitSignal chan struct{}
}

func NewMonitoring(client MonitoringSender, endpoint string, tags map[string]string, heartbeatInterval time.Duration, alertChan chan error) *Monitoring {
	return &Monitoring{
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
	go func() {
		reportTime := time.Now().UTC().Add(m.heartbeatInterval)

	OutterLoop:
		for {
			if time.Now().UTC().After(reportTime) {
				m.log.Info("Sending heartbeat")
				if m.client != nil {
					req := m.prepareHeartbeatEventRequest()
					_, err := m.client.Do(req)
					if err != nil {
						m.log.Warnf("failed to send heartbeat event: %s", err)
					}
				}
				reportTime = time.Now().UTC().Add(m.heartbeatInterval)
			}

			select {
			case <-m.exitSignal:
				m.log.Info("Monitoring is shutting down")
				break OutterLoop
			case err := <-m.alertChan:
				m.log.Info("Sending alert")
				if m.client != nil {
					req := m.prepareAlertEventRequest(err)
					_, err := m.client.Do(req)
					if err != nil {
						m.log.Warnf("failed to send alert event: %s", err)
					}
				}
			default:
			}
		}
	}()
}

func (m *Monitoring) Stop() {
	m.exitSignal <- struct{}{}
}

func (m *Monitoring) prepareHeartbeatEventRequest() *http.Request {
	return &http.Request{
		Method: "POST",
		URL: &url.URL{
			Scheme: "https",
			Host:   m.endpoint,
		},
	}
}

func (m *Monitoring) prepareAlertEventRequest(_ error) *http.Request {
	return &http.Request{
		Method: "POST",
		URL: &url.URL{
			Scheme: "https",
			Host:   m.endpoint,
		},
	}
}
