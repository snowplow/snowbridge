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

func TestHTTP_Templating_WithPrettyPrint(t *testing.T) {
	assert := assert.New(t)

	rawTemplate := `
  {
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

	templated, goodMessages, invalidMessages := target.renderBatchUsingTemplate(inputMessages)

	expectedOutput := `
  { 
    "attributes": [1,2,3],
    "events": [{"nested":"value1"},{"nested":"value2"},{"nested":"value3"}]
  }`
	assert.JSONEq(expectedOutput, string(templated))
	assert.Equal(inputMessages, goodMessages)
	assert.Empty(invalidMessages)
}

func TestHTTP_Templating_NoPrettyPrinting(t *testing.T) {
	assert := assert.New(t)

	rawTemplate := `
  {
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

	templated, goodMessages, invalidMessages := target.renderBatchUsingTemplate(inputMessages)

	//we get a stringified map for JSON
	expectedOutput := `
  {
    "attributes": [1,2,3],
    "events": [map[nested:value1],map[nested:value2],map[nested:value3]]
  }`
	assert.Equal(expectedOutput, string(templated))
	assert.Equal(inputMessages, goodMessages)
	assert.Empty(invalidMessages)
}

func TestHTTP_Templating_WithEnvVariable(t *testing.T) {
	assert := assert.New(t)

	t.Setenv("API_KEY", "supersecret")

	rawTemplate := `
  {
    "api_key": "{{ env "API_KEY" }}",
    "events": [{{range $i, $data := .}}{{if $i}},{{end}}{{prettyPrint .}}{{end}}]
  }`

	parsedTemplate, err := parseRequestTemplate(rawTemplate)
	assert.Nil(err)
	target := HTTPTarget{requestTemplate: parsedTemplate}

	inputMessages := []*models.Message{
		{Data: []byte(`{ "data": 1}`)},
		{Data: []byte(`{ "data": 2}`)},
	}

	templated, goodMessages, invalidMessages := target.renderBatchUsingTemplate(inputMessages)

	expectedOutput := `
  {
    "api_key": "supersecret",
    "events": [{"data":1},{"data":2}]
  }`
	assert.JSONEq(expectedOutput, string(templated))
	assert.Equal(inputMessages, goodMessages)
	assert.Empty(invalidMessages)
}

func TestHTTP_Templating_ArrayProvided(t *testing.T) {
	assert := assert.New(t)

	rawTemplate := `
  {
    "attributes": [{{range $i, $data := .}}{{if $i}},{{end}}{{range $i, $d := $data}}{{if $i}},{{end}}"Value: {{$d}}"{{end}}{{end}}]
  }`

	parsedTemplate, err := parseRequestTemplate(rawTemplate)
	assert.Nil(err)
	target := HTTPTarget{requestTemplate: parsedTemplate}

	inputMessages := []*models.Message{
		{Data: []byte(`["value1", "value2", "value3"]`)},
	}

	templated, goodMessages, invalidMessages := target.renderBatchUsingTemplate(inputMessages)

	expectedOutput := `
  {
    "attributes": ["Value: value1","Value: value2","Value: value3"]
  }`
	assert.JSONEq(expectedOutput, string(templated))
	assert.Equal(inputMessages, goodMessages)
	assert.Empty(invalidMessages)
}

func TestHTTP_Templating_AccessNonExistingField(t *testing.T) {
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

			templated, goodMessages, invalidMessages := target.renderBatchUsingTemplate(inputMessages)

			assert.Equal(tt.Output, string(templated))
			assert.Equal(inputMessages, goodMessages)
			assert.Empty(invalidMessages)

		})
	}
}

func TestHTTP_Templatating_ParsingTemplateFailure(t *testing.T) {
	assert := assert.New(t)

	rawTemplate := "{{ "
	_, err := parseRequestTemplate(rawTemplate)
	assert.Equal("template: HTTP:1: unclosed action", err.Error())
}

func TestHTTP_Templating_JSONParseFailure(t *testing.T) {
	assert := assert.New(t)

	rawTemplate := "{{ prettyPrint (index . 0).event_data}}"

	parsedTemplate, err := parseRequestTemplate(rawTemplate)
	assert.Nil(err)
	target := HTTPTarget{requestTemplate: parsedTemplate}

	inputMessages := []*models.Message{
		{Data: []byte(`{ "event_data": { "nested": "value1"}}`)},
		{Data: []byte(`plain string, can't unmarshall`)},
	}

	templated, goodMessages, invalidMessages := target.renderBatchUsingTemplate(inputMessages)

	assert.JSONEq(`{"nested":"value1"}`, string(templated))

	assert.Len(goodMessages, 1)
	assert.Len(invalidMessages, 1)

	//only the first one from input is good
	assert.Equal(inputMessages[0], goodMessages[0])

	//and the second one is invalid
	assert.Equal(inputMessages[1], invalidMessages[0])
}

func TestHTTP_Templating_RenderFailure(t *testing.T) {
	assert := assert.New(t)

	rawTemplate := "{{ index . 1 }}"
	parsedTemplate, err := parseRequestTemplate(rawTemplate)
	assert.Nil(err)
	target := HTTPTarget{requestTemplate: parsedTemplate}

	inputMessages := []*models.Message{
		{Data: []byte(`{ "event_data": { "nested": "value1"}, "attribute_data": 1}`)},
	}

	templated, goodMessages, invalidMessages := target.renderBatchUsingTemplate(inputMessages)

	expectedError := `Could not create request JSON: template: HTTP:1:3: executing "HTTP" at <index . 1>: error calling index: reflect: slice index out of range`
	assert.Equal(expectedError, invalidMessages[0].GetError().Error())
	assert.Empty(templated)
	assert.Empty(goodMessages)
	assert.Equal(inputMessages, invalidMessages)
}
