package statsreceiver

import (
	"github.com/snowplow/snowbridge/pkg/models"
	"net/http"
)

type MetadataReporterConfig struct {
}

type metadataReporter struct {
	client *http.Client
}

func NewMetadataReporter(config MetadataReporterConfig) *metadataReporter {
	return &metadataReporter{}
}

type Event struct {
	Success       int64             `json:"successCount"`
	Filter        int64             `json:"filterCount"`
	Failed        int64             `json:"failedCount"`
	Invalid       int64             `json:"invalidCount"`
	InvalidErrors []AggregatedError `json:"invalidErrors"`
	FailedErrors  []AggregatedError `json:"failedErrors"` // transient/retryable
}

type AggregatedError struct {
	Code        string `json:"code"`
	Description string `json:"description"`
	Count       int    `json:"count"`
}

func (s *metadataReporter) Send(b *models.ObserverBuffer) {

	// Schema design: https://www.notion.so/keep-in-the-snow/Snowbridge-metadata-metrics-reporting-21b07af295a28010af25dff43b628093
	// See option 1 as preferred design.

	//TODO Aggregate invalid and failed errors from the buffer and send event!
	//event := Event{
	//	Success: b.MsgSent,
	//	Filter:  b.MsgFiltered,
	//	Failed:  b.MsgFailed,
	//	Invalid: b.InvalidMsgSent,
	//	InvalidErrors: [],  //fill this field
	//	FailedErrors: [], //fill this field
	//}
}
