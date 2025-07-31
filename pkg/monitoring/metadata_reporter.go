package monitoring

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/snowplow/snowbridge/pkg/models"
)

// aggregatedError holds aggregated error information
// for reporting by metadata reporter
type aggregatedError struct {
	Code        string `json:"code"`
	Description string `json:"description"`
	Count       int    `json:"count"`
}

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
	Filtered      int64             `json:"filteredCount"`
	Failed        int64             `json:"failedCount"`
	Invalid       int64             `json:"invalidCount"`
	InvalidErrors []aggregatedError `json:"invalidErrors,omitempty"`
	FailedErrors  []aggregatedError `json:"failedErrors,omitempty"` // transient/retryable
	Tags          map[string]string `json:"tags"`
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
			Filtered:      b.MsgFiltered,
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

func aggregateErrors(errsMap map[models.MetadataCodeDescription]int) []aggregatedError {
	var aggrErrors []aggregatedError

	for err, v := range errsMap {
		aggrErrors = append(aggrErrors, aggregatedError{
			Code:        err.Code,
			Description: err.Description,
			Count:       v,
		})
	}
	return aggrErrors
}
