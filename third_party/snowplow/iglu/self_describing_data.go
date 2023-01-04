//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

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
