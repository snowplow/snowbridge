// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package transformconfig

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/snowplow-devops/stream-replicator/pkg/transform/engine"
)

func TestMkEngineFunction(t *testing.T) {
	var eng engine.Engine
	eng = &engine.JSEngine{
		Name:       "test-engine",
		Code:       nil,
		RunTimeout: 15,
		SpMode:     false,
	}
	testCases := []struct {
		Name           string
		Engines        []engine.Engine
		Transformation *Transformation
		ExpectedErr    error
	}{
		{
			Name:    "no engines",
			Engines: nil,
			Transformation: &Transformation{
				Name:       "js",
				EngineName: "test-engine",
			},
			ExpectedErr: fmt.Errorf("could not find engine named test-engine"),
		},
		{
			Name:    "success",
			Engines: []engine.Engine{eng},
			Transformation: &Transformation{
				Name:       "js",
				EngineName: "test-engine",
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			fun, err := MkEngineFunction(tt.Engines, tt.Transformation)

			if tt.ExpectedErr != nil {
				assert.Equal(tt.ExpectedErr.Error(), err.Error())
				assert.Nil(fun)
			} else {
				assert.Nil(err)
				assert.NotNil(fun)
			}
		})
	}
}

func TestValidateTransformations(t *testing.T) {
	testCases := []struct {
		Name            string
		Transformations []*Transformation
		ExpectedErrs    []error
	}{
		{
			Name: "invalid name",
			Transformations: []*Transformation{{
				Name: "wrongName",
			}},
			ExpectedErrs: []error{fmt.Errorf("invalid transformation name: wrongName")},
		},
		{
			Name: "spEnrichedSetPk success",
			Transformations: []*Transformation{{
				Name:   "spEnrichedSetPk",
				Option: `app_id`,
			}},
		},
		{
			Name: "spEnrichedSetPk no option",
			Transformations: []*Transformation{{
				Name: "spEnrichedSetPk",
			}},
			ExpectedErrs: []error{fmt.Errorf("error validating transformation #0 spEnrichedSetPk, empty option")},
		},
		{
			Name: "spEnrichedFilter success",
			Transformations: []*Transformation{{
				Name:  "spEnrichedFilter",
				Field: "app_id",
				Regex: "test.+",
			}},
		},
		{
			Name: "spEnrichedFilter regexp does not compile",
			Transformations: []*Transformation{{
				Name:  "spEnrichedFilter",
				Field: "app_id",
				Regex: "?(?=-)",
			}},
			ExpectedErrs: []error{fmt.Errorf("error validating transformation #0 spEnrichedFilter, regex does not compile. error: error parsing regexp: missing argument to repetition operator: `?`")},
		},
		{
			Name: "spEnrichedFilter empty field",
			Transformations: []*Transformation{{
				Name:  "spEnrichedFilter",
				Regex: "test.+",
			}},
			ExpectedErrs: []error{fmt.Errorf("error validating transformation #0 spEnrichedFilter, empty field")},
		},
		{
			Name: "spEnrichedFilter empty regex",
			Transformations: []*Transformation{{
				Name:  "spEnrichedFilter",
				Field: "app_id",
			}},
			ExpectedErrs: []error{fmt.Errorf("error validating transformation #0 spEnrichedFilter, empty regex")},
		},
		{
			Name: "spEnrichedFilterContext success",
			Transformations: []*Transformation{{
				Name:  "spEnrichedFilterContext",
				Field: "contexts_nl_basjes_yauaa_context_1.test1.test2[0]",
				Regex: "test.+",
			}},
		},
		{
			Name: "spEnrichedFilterContext regexp does not compile",
			Transformations: []*Transformation{{
				Name:  "spEnrichedFilterContext",
				Field: "contexts_nl_basjes_yauaa_context_1.test1.test2[0]",
				Regex: "?(?=-)",
			}},
			ExpectedErrs: []error{fmt.Errorf("error validating transformation #0 spEnrichedFilterContext, regex does not compile. error: error parsing regexp: missing argument to repetition operator: `?`")},
		},
		{
			Name: "spEnrichedFilterContext empty field",
			Transformations: []*Transformation{{
				Name:  "spEnrichedFilterContext",
				Regex: "test.+",
			}},
			ExpectedErrs: []error{fmt.Errorf("error validating transformation #0 spEnrichedFilterContext, empty field")},
		},
		{
			Name: "spEnrichedFilterContext empty regex",
			Transformations: []*Transformation{{
				Name:  "spEnrichedFilterContext",
				Field: "contexts_nl_basjes_yauaa_context_1.test1.test2[0]",
			}},
			ExpectedErrs: []error{fmt.Errorf("error validating transformation #0 spEnrichedFilterContext, empty regex")},
		},
		{
			Name: "spEnrichedFilterUnstructEvent success",
			Transformations: []*Transformation{{
				Name:  "spEnrichedFilterUnstructEvent",
				Field: "unstruct_event_add_to_cart_1.sku",
				Regex: "test.+",
			}},
		},
		{
			Name: "spEnrichedFilterUnstructEvent regexp does not compile",
			Transformations: []*Transformation{{
				Name:  "spEnrichedFilterUnstructEvent",
				Field: "unstruct_event_add_to_cart_1.sku",
				Regex: "?(?=-)",
			}},
			ExpectedErrs: []error{fmt.Errorf("error validating transformation #0 spEnrichedFilterUnstructEvent, regex does not compile. error: error parsing regexp: missing argument to repetition operator: `?`")},
		},
		{
			Name: "spEnrichedFilterUnstructEvent empty field",
			Transformations: []*Transformation{{
				Name:  "spEnrichedFilterUnstructEvent",
				Regex: "test.+",
			}},
			ExpectedErrs: []error{fmt.Errorf("error validating transformation #0 spEnrichedFilterUnstructEvent, empty field")},
		},
		{
			Name: "spEnrichedFilterUnstructEvent empty regex",
			Transformations: []*Transformation{{
				Name:  "spEnrichedFilterUnstructEvent",
				Field: "unstruct_event_add_to_cart_1.sku",
			}},
			ExpectedErrs: []error{fmt.Errorf("error validating transformation #0 spEnrichedFilterUnstructEvent, empty regex")},
		},
		{
			Name: "lua success",
			Transformations: []*Transformation{{
				Name:       "lua",
				EngineName: "test-engine",
			}},
		},
		{
			Name: "lua no engine name",
			Transformations: []*Transformation{{
				Name: "lua",
			}},
			ExpectedErrs: []error{fmt.Errorf("error validating lua transformation #0, empty engine name")},
		},
		{
			Name: "js success",
			Transformations: []*Transformation{{
				Name:       "js",
				EngineName: "test-engine",
			}},
		},
		{
			Name: "js no engine name",
			Transformations: []*Transformation{{
				Name: "js",
			}},
			ExpectedErrs: []error{fmt.Errorf("error validating js transformation #0, empty engine name")},
		},
		{
			Name: "multiple validation errors",
			Transformations: []*Transformation{
				{
					Name: "js",
				},
				{
					Name:  "spEnrichedFilter",
					Regex: "test.+",
				},
				// a successful transformation mixed in to test transformation counter
				{
					Name: "spEnrichedToJson",
				},
				{
					Name: "spEnrichedSetPk",
				},
			},
			ExpectedErrs: []error{
				fmt.Errorf("error validating js transformation #0, empty engine name"),
				fmt.Errorf("error validating transformation #1 spEnrichedFilter, empty field"),
				fmt.Errorf("error validating transformation #3 spEnrichedSetPk, empty option"),
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			valErrs := ValidateTransformations(tt.Transformations)

			if tt.ExpectedErrs != nil {
				for idx, valErr := range valErrs {
					assert.Equal(valErr.Error(), tt.ExpectedErrs[idx].Error())
				}
			} else {
				assert.Nil(valErrs)
			}
		})
	}
}
