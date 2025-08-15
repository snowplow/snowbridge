package monitoring

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/snowplow/snowbridge/v3/pkg/models"
)

// AggregatedError holds aggregated error information
// for reporting by metadata reporter
type AggregatedError struct {
	Code        string `json:"code"`
	Description string `json:"description"`
	Count       int    `json:"count"`
}

// MetadataSender describes the interface for how to send metadata events
type MetadataSender interface {
	Do(req *http.Request) (*http.Response, error)
}

type MetadataReporterer interface {
	Send(b *models.ObserverBuffer, periodStart, periodEnd time.Time)
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
	InvalidErrors []AggregatedError `json:"invalidErrors,omitempty"`
	FailedErrors  []AggregatedError `json:"failedErrors,omitempty"` // transient/retryable
	Tags          map[string]string `json:"tags"`
}

func (mr *MetadataReporter) Send(b *models.ObserverBuffer, periodStart, periodEnd time.Time) {
	aggrInvalid := errorsMapToSlice(b.InvalidErrors)
	aggrFailed := errorsMapToSlice(b.FailedErrors)

	event := MetadataEvent{
		Schema: "iglu:com.snowplowanalytics.snowplow/event_forwarding_metrics/jsonschema/1-0-0",
		Data: MetadataWrapper{
			AppName:       mr.appName,
			AppVersion:    mr.appVersion,
			PeriodStart:   periodStart.Format(time.RFC3339),
			PeriodEnd:     periodEnd.Format(time.RFC3339),
			Success:       b.MsgSent,
			Filtered:      b.MsgFiltered,
			Failed:        b.MsgFailed,
			Invalid:       b.InvalidMsgSent,
			InvalidErrors: aggrInvalid,
			FailedErrors:  aggrFailed,
			Tags:          mr.tags,
		},
	}

	header := http.Header{}
	header.Add("Content-Type", "application/json")

	var body bytes.Buffer
	err := json.NewEncoder(&body).Encode(event)
	if err != nil {
		mr.log.Errorf("failed to marshall event: %s", err)
		return
	}

	req, err := http.NewRequest(
		http.MethodPost,
		mr.endpoint,
		&body,
	)
	if err != nil {
		mr.log.Errorf("failed to create POST request: %s", err)
		return
	}
	req.Header = header

	if _, err := mr.client.Do(req); err != nil {
		mr.log.Errorf("failed to send metadata event: %s", err)
		return
	}
}

func errorsMapToSlice(errsMap map[models.MetadataCodeDescription]int) []AggregatedError {
	var aggrErrors []AggregatedError

	for err, v := range errsMap {
		aggrErrors = append(aggrErrors, AggregatedError{
			Code:        err.Code,
			Description: err.Description,
			Count:       v,
		})
	}
	return aggrErrors
}
