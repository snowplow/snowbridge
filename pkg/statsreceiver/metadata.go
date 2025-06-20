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

func (s *metadataReporter) Send(b *models.ObserverBuffer) {
	metadata := b.ErrorsMetadata
	doSomeMagic(metadata.Invalid)   // it's a list now, so aggregate/group with counts here (or do it early when appending write results in a buffer)
	doSomeMagic(metadata.Retryable) // same here

	/* TODO how should event we send here look like?
		Unstruct event with a bunch of contexts/entities, each representing counts for each error type/category? We already have all details in the 'metadata' above.
		So something like this for context:
		{
			"type": "Retryable/Invalid", // if we need this type of info at all
			"source": "Transformation/API",
		    "code": "JSRuntimeError"/"400 Bad request",
		    "count": 100
		}

		If we want to also include success/filtered, we could do that by reusing the above structure (making 'source' and 'code' optional):
		{
			"type": "Success",
		    "count": 100
		}

		See existing schemas for metadata reporting used by enrich:
	      -
	*/
}
