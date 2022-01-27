// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package iglu

import (
	"encoding/json"
)

// SelfDescribingData describes a Snowplow SelfDescribing JSON object
// which encompasses a schema key and a data payload
type SelfDescribingData struct {
	schema string
	data   interface{}
}

// NewSelfDescribingData creates a new SDJ struct.
func NewSelfDescribingData(schema string, data interface{}) *SelfDescribingData {
	return &SelfDescribingData{schema: schema, data: data}
}

// Get wraps the schema and data into a map.
func (s SelfDescribingData) Get() map[string]interface{} {
	return map[string]interface{}{
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
