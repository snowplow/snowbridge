/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package iglu

import (
	"encoding/json"
)

// SelfDescribingData describes a Snowplow SelfDescribing JSON object
// which encompasses a schema key and a data payload
type SelfDescribingData struct {
	schema string
	data   any
}

// NewSelfDescribingData creates a new SDJ struct.
func NewSelfDescribingData(schema string, data any) *SelfDescribingData {
	return &SelfDescribingData{schema: schema, data: data}
}

// Get wraps the schema and data into a map.
func (s SelfDescribingData) Get() map[string]any {
	return map[string]any{
		"schema": s.schema,
		"data":   s.data,
	}
}

// Get wraps the schema and data into a map.
func (s SelfDescribingData) String() (string, error) {
	b, err := json.Marshal(s.Get())
	if err != nil {
		return "", err
	}
	return string(b), nil
}
