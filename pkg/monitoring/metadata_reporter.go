package monitoring

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/snowplow/snowbridge/pkg/models"
)

// MetadataSender describes the interface for how to send metadata events
type MetadataSender interface {
	Do(req *http.Request) (*http.Response, error)
}

type MetadataReporter struct {
	client   MetadataSender
	endpoint string
	log      *logrus.Entry

	appName    string
	appVersion string
	tags       map[string]string
}

func NewMetadataReporter(appName, appVersion string, client MetadataSender, endpoint string, tags map[string]string) *MetadataReporter {
	return &MetadataReporter{
		appName:    appName,
		appVersion: appVersion,
		client:     client,
		endpoint:   endpoint,
		tags:       tags,
		log:        logrus.WithFields(logrus.Fields{"name": "MetadataReporter"}),
	}
}

// Schema design: https://www.notion.so/keep-in-the-snow/Snowbridge-metadata-metrics-reporting-21b07af295a28010af25dff43b628093
// See option 1 as preferred design.
type MetadataEvent struct {
	Schema string          `json:"schema"`
	Data   MetadataWrapper `json:"data"`
}

type MetadataWrapper struct {
	AppName       string            `json:"appName"`
	AppVersion    string            `json:"appVersion"`
	PeriodStart   string            `json:"periodStart"`
	PeriodEnd     string            `json:"periodEnd"`
	Success       int64             `json:"successCount"`
	Filter        int64             `json:"filterCount"`
	Failed        int64             `json:"failedCount"`
	Invalid       int64             `json:"invalidCount"`
	InvalidErrors []AggregatedError `json:"invalidErrors,omitempty"`
	FailedErrors  []AggregatedError `json:"failedErrors,omitempty"` // transient/retryable
	Tags          map[string]string `json:"tags"`
}

type AggregatedError struct {
	Code        string `json:"code"`
	Description string `json:"description"`
	Count       int    `json:"count"`
}

func (s *MetadataReporter) Send(b *models.ObserverBuffer, periodStart, periodEnd time.Time) {
	aggrInvalid := aggregateErrors(b.InvalidErrors)
	aggrFailed := aggregateErrors(b.FailedErrors)

	event := MetadataEvent{
		Schema: "iglu:com.snowplowanalytics.snowplow/event_forwarding_metrics/jsonschema/1-0-0",
		Data: MetadataWrapper{
			AppName:       s.appName,
			AppVersion:    s.appVersion,
			PeriodStart:   periodStart.Format(time.RFC3339),
			PeriodEnd:     periodEnd.Format(time.RFC3339),
			Success:       b.MsgSent,
			Filter:        b.MsgFiltered,
			Failed:        b.MsgFailed,
			Invalid:       b.InvalidMsgSent,
			InvalidErrors: aggrInvalid,
			FailedErrors:  aggrFailed,
			Tags:          s.tags,
		},
	}

	header := http.Header{}
	header.Add("Content-Type", "application/json")

	var body bytes.Buffer
	err := json.NewEncoder(&body).Encode(event)
	if err != nil {
		s.log.Errorf("failed to marshall event: %s", err)
		return
	}

	req, err := http.NewRequest(
		http.MethodPost,
		s.endpoint,
		&body,
	)
	if err != nil {
		s.log.Errorf("failed to create POST request: %s", err)
		return
	}
	req.Header = header

	if _, err := s.client.Do(req); err != nil {
		s.log.Errorf("failed to send metadata event: %s", err)
		return
	}
}

func aggregateErrors(errs []models.SanitisedErrorMetadata) []AggregatedError {
	tempAggrMap := make(map[string]int)

	for _, err := range errs {
		fmt.Println(err, tempAggrMap)
		tempAggrMap[err.Code()] = tempAggrMap[err.Code()] + 1
	}

	var aggrErrors []AggregatedError
	for k, v := range tempAggrMap {
		aggrErrors = append(aggrErrors, AggregatedError{
			Code:  k,
			Count: v,
		})
	}
	return aggrErrors
}
