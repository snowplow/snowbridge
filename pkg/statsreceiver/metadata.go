package statsreceiver

import (
	"github.com/pkg/errors"
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

func (s *metadataReporter) Send(b *models.ObserverBuffer) {

	// Schema design: https://www.notion.so/keep-in-the-snow/Snowbridge-metadata-metrics-reporting-21b07af295a28010af25dff43b628093

	// Number of successful messages...
	b.MsgSent
	// Number of filtered messages...
	b.MsgFiltered

	// Retryable errors details, group by source/code and then count for each group
	for _, err := range b.RetryableErrors {
		var em models.ErrorMetadata
		if errors.As(err, &em) {
			//include source and code
			em.MetadataSource()
			em.ReportableCode()
		}
	}

	// Same for invalid errors....
	for _, err := range b.InvalidErrors {
		var em models.ErrorMetadata
		if errors.As(err, &em) {
			//include source and code
			em.MetadataSource()
			em.ReportableCode()
		}
	}
}
