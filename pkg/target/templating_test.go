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

package target

import (
	"testing"

	"github.com/snowplow/snowbridge/pkg/models"
	"github.com/stretchr/testify/assert"
)

func TestTemplating_WithPrettyPrint(t *testing.T) {
	assert := assert.New(t)

	rawTemplate :=
		`{
  "attributes": [{{range $i, $data := .}}{{if $i}},{{end}}{{.attribute_data}}{{end}}],
  "events": [{{range $i, $data := .}}{{if $i}},{{end}}{{prettyPrint .event_data}}{{end}}]
}`

	parsedTemplate, err := parseRequestTemplate(rawTemplate)
	assert.Nil(err)
	target := HTTPTarget{requestTemplate: parsedTemplate}

	inputMessages := []*models.Message{
		{Data: []byte(`{ "event_data": { "nested": "value1"}, "attribute_data": 1}`)},
		{Data: []byte(`{ "event_data": { "nested": "value2"}, "attribute_data": 2}`)},
		{Data: []byte(`{ "event_data": { "nested": "value3"}, "attribute_data": 3}`)},
	}

	templated, goodMessages, invalidMessages, err := target.renderBatchUsingTemplate(inputMessages)
	assert.Nil(err)

	expectedOutput := "{\n  \"attributes\": [1,2,3],\n  \"events\": [{\"nested\":\"value1\"},{\"nested\":\"value2\"},{\"nested\":\"value3\"}]\n}"
	assert.Equal(expectedOutput, string(templated))
	assert.Equal(inputMessages, goodMessages)
	assert.Empty(invalidMessages)
}

func TestTemplating_NoPrettyPrinting(t *testing.T) {
	assert := assert.New(t)

	rawTemplate :=
		`{
  "attributes": [{{range $i, $data := .}}{{if $i}},{{end}}{{.attribute_data}}{{end}}],
  "events": [{{range $i, $data := .}}{{if $i}},{{end}}{{.event_data}}{{end}}]
}`

	parsedTemplate, err := parseRequestTemplate(rawTemplate)
	assert.Nil(err)
	target := HTTPTarget{requestTemplate: parsedTemplate}

	inputMessages := []*models.Message{
		{Data: []byte(`{ "event_data": { "nested": "value1"}, "attribute_data": 1}`)},
		{Data: []byte(`{ "event_data": { "nested": "value2"}, "attribute_data": 2}`)},
		{Data: []byte(`{ "event_data": { "nested": "value3"}, "attribute_data": 3}`)},
	}

	templated, goodMessages, invalidMessages, err := target.renderBatchUsingTemplate(inputMessages)
	assert.Nil(err)

	//we get a stringified map for JSON
	expectedOutput := "{\n  \"attributes\": [1,2,3],\n  \"events\": [map[nested:value1],map[nested:value2],map[nested:value3]]\n}"
	assert.Equal(expectedOutput, string(templated))
	assert.Equal(inputMessages, goodMessages)
	assert.Empty(invalidMessages)
}

func TestTemplating_AccessNonExistingField(t *testing.T) {
	noPretty := "{{ (index . 0).nonexistent}}"
	pretty := "{{ prettyPrint (index . 0).nonexistent}}"

	testCases := []struct {
		Name     string
		Template string
		Input    string
		Output   string
	}{
		{Name: "JSON - pretty", Template: pretty, Input: `{ "existing": { "nested": "value1"}}`, Output: "null"},
		{Name: "JSON - no pretty", Template: noPretty, Input: `{ "existing": { "nested": "value1"}}`, Output: "<no value>"},
		{Name: "Regular value - pretty", Template: pretty, Input: `{ "existing": 100}`, Output: "null"},
		{Name: "Regular value - no pretty", Template: noPretty, Input: `{ "existing": 100}`, Output: "<no value>"},
		{Name: "Null value - pretty", Template: pretty, Input: `{ "existing": null}`, Output: "null"},
		{Name: "Null value - no pretty", Template: noPretty, Input: `{ "existing": null}`, Output: "<no value>"},
	}
	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {

			assert := assert.New(t)
			parsedTemplate, err := parseRequestTemplate(tt.Template)
			assert.Nil(err)
			target := HTTPTarget{requestTemplate: parsedTemplate}

			inputMessages := []*models.Message{{Data: []byte(tt.Input)}}

			templated, goodMessages, invalidMessages, err := target.renderBatchUsingTemplate(inputMessages)
			assert.Nil(err)

			assert.Equal(tt.Output, string(templated))
			assert.Equal(inputMessages, goodMessages)
			assert.Empty(invalidMessages)

		})
	}
}

func TestTemplatating_ParsingTemplateFailure(t *testing.T) {
	assert := assert.New(t)

	rawTemplate := "{{ "
	_, err := parseRequestTemplate(rawTemplate)
	assert.Equal("template: HTTP:1: unclosed action", err.Error())
}

func TestTemplating_JSONParseFailure(t *testing.T) {
	assert := assert.New(t)

	rawTemplate := "{{ prettyPrint (index . 0).event_data}}"

	parsedTemplate, err := parseRequestTemplate(rawTemplate)
	assert.Nil(err)
	target := HTTPTarget{requestTemplate: parsedTemplate}

	inputMessages := []*models.Message{
		{Data: []byte(`{ "event_data": { "nested": "value1"}}`)},
		{Data: []byte(`plain string, can't parse as map[string]interface{}`)},
	}

	templated, goodMessages, invalidMessages, err := target.renderBatchUsingTemplate(inputMessages)
	assert.Nil(err)

	assert.Equal("{\"nested\":\"value1\"}", string(templated))

	assert.Len(goodMessages, 1)
	assert.Len(invalidMessages, 1)

	//only the first one from input is good
	assert.Equal(inputMessages[0], goodMessages[0])

	//and the second one is invalid
	assert.Equal(inputMessages[1], invalidMessages[0])
}

func TestTemplating_RenderFailure(t *testing.T) {
	assert := assert.New(t)

	rawTemplate := "{{ index . 1 }}"
	parsedTemplate, err := parseRequestTemplate(rawTemplate)
	assert.Nil(err)
	target := HTTPTarget{requestTemplate: parsedTemplate}

	inputMessages := []*models.Message{
		{Data: []byte(`{ "event_data": { "nested": "value1"}, "attribute_data": 1}`)},
	}

	templated, goodMessages, invalidMessages, err := target.renderBatchUsingTemplate(inputMessages)

	expectedError := `template: HTTP:1:3: executing "HTTP" at <index . 1>: error calling index: reflect: slice index out of range`
	assert.Equal(expectedError, err.Error())
	assert.Empty(templated)
	assert.Empty(goodMessages)
	assert.Equal(inputMessages, invalidMessages)
}
