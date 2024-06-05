/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package batchtransform

import (
	"bytes"
	"encoding/json"
	"text/template"

	"github.com/pkg/errors"
	"github.com/snowplow/snowbridge/pkg/models"
)

// TemplaterBatchTransformationFunction is a thing TODO add desc
func TemplaterBatchTransformationFunction(batches []models.MessageBatch) ([]models.MessageBatch, []*models.Message) {

	// This is just an outline implementation of a templater function, to help figure out the design of batch transforms in general

	// The templater would fit here along the following lines:
	const templ = `{
		attributes: [ {{$first_1 := true}}
		  {{range .}}{{if $first_1}}{{$first_1 = false}}{{else}},{{end}}
		  {{printf "%s" .attribute_data}}{{end}}
		  ],
		events: [ {{$first_2 := true}}
		  {{range .}}{{if $first_2}}{{$first_2 = false}}{{else}},{{end}}
		  {{printf "%s" .event_data}}{{end}}
		  ]
	  }`

	invalid := make([]*models.Message, 0)
	safe := make([]*models.Message, 0)

	for _, b := range batches {
		formatted := []map[string]json.RawMessage{}
		for _, msg := range b.OriginalMessages {
			// Use json.RawMessage to ensure templating format works (real implementation has a problem to figure out here)
			var asMap map[string]json.RawMessage

			if err := json.Unmarshal(msg.Data, &asMap); err != nil {
				msg.SetError(errors.Wrap(err, "templater error")) // TODO: Cleanup!
				invalid = append(invalid, msg)
				continue
			}

			formatted = append(formatted, asMap)
		}
		var buf bytes.Buffer

		t := template.Must(template.New("example").Parse(templ))
		if err := t.Execute(&buf, formatted); err != nil {
			for _, msg := range safe {
				msg.SetError(errors.Wrap(err, "templater error")) // TODO: Cleanup!
				invalid = append(invalid, msg)
			}
			return nil, invalid
		}

		// Assign the templated request to the HTTPRequestBody field
		b.BatchData = buf.Bytes()

	}

	return batches, invalid
}
