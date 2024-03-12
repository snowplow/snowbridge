package transform

import (
	"errors"

	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/models"

	"github.com/snowplow/snowplow-golang-analytics-sdk/analytics"
)

// The gtmssPreviewAdapter implements the Pluggable interface
type gtmssPreviewAdapter func(i interface{}) (interface{}, error)

// ProvideDefault implements the ComponentConfigurable interface
func (f gtmssPreviewAdapter) ProvideDefault() (interface{}, error) {
	return nil, nil
}

// Create implements the ComponentCreator interface.
func (f gtmssPreviewAdapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// gtmssPreviewAdapterGenerator returns a gtmssPreviewAdapter
func gtmssPreviewAdapterGenerator(f func() (TransformationFunction, error)) gtmssPreviewAdapter {
	return func(i interface{}) (interface{}, error) {
		if i != nil {
			return nil, errors.New("unexpected configuration input for gtmssPreview transformation")
		}

		return f()
	}
}

// gtmssPreviewConfigFunction returns a transformation function
func gtmssPreviewConfigFunction() (TransformationFunction, error) {
	ctx := "contexts_com_google_tag-manager_server-side_preview_mode_1"
	property := "x-gtm-server-preview"
	header := "x-gtm-server-preview"
	return gtmssPreviewTransformation(ctx, property, header), nil
}

// GTMSSPreviewConfigPair is the configuration pair for the gtmss preview transformation
var GTMSSPreviewConfigPair = config.ConfigurationPair{
	Name:   "gtmssPreview",
	Handle: gtmssPreviewAdapterGenerator(gtmssPreviewConfigFunction),
}

// gtmssPreviewTransformation returns a transformation function
func gtmssPreviewTransformation(ctx, property, headerKey string) TransformationFunction {
	return func(message *models.Message, interState interface{}) (*models.Message, *models.Message, *models.Message, interface{}) {
		parsedEvent, err := IntermediateAsSpEnrichedParsed(interState, message)
		if err != nil {
			message.SetError(err)
			return nil, nil, message, nil
		}

		headerVal, err := extractHeaderValue(parsedEvent, ctx, property)
		if err != nil {
			message.SetError(err)
			return nil, nil, message, nil
		}
		if headerVal != nil {
			if message.HTTPHeaders == nil {
				message.HTTPHeaders = make(map[string][]string)
			}
			message.HTTPHeaders[headerKey] = append(message.HTTPHeaders[headerKey], *headerVal)
			return message, nil, nil, parsedEvent
		}

		return message, nil, nil, parsedEvent
	}
}

func extractHeaderValue(parsedEvent analytics.ParsedEvent, ctx, prop string) (*string, error) {
	spMap, err := parsedEvent.ToMap()
	if err != nil {
		return nil, err
	}

	gtmssPreview, ok := spMap[ctx]
	if !ok {
		// not for preview mode, so do nothing
		return nil, nil
	}

	gtmssPreviewData, ok := gtmssPreview.([]interface{})
	if !ok {
		// this is generally not expected to happen
		return nil, errors.New("invalid gtmss preview context")
	}

	if len(gtmssPreviewData) > 0 {
		previewMode, ok := gtmssPreviewData[0].(map[string]interface{})
		if !ok {
			// this is generally not expected to happen
			return nil, errors.New("invalid gtmss preview context data")
		}

		previewHeader, ok := previewMode[prop]
		if !ok {
			return nil, errors.New("missing header property")
		}

		headerVal, ok := previewHeader.(string)
		if !ok {
			return nil, errors.New("invalid header value")
		}

		return &headerVal, nil
	}

	return nil, errors.New("empty gtmss preview context")
}
