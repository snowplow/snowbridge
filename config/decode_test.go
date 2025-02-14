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

package config

import (
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclparse"
	"github.com/stretchr/testify/assert"
)

type testStruct struct {
	Test string `hcl:"test_string" env:"TEST_STRING"`
}

func TestHclDecode(t *testing.T) {
	evalCtx := &hcl.EvalContext{}
	hclDecoder := hclDecoder{evalCtx}
	hclSrc := `
test_string = "ateststring"
`
	p := hclparse.NewParser()
	hclFile, diags := p.ParseHCL([]byte(hclSrc), "placeholder.hcl")
	if diags.HasErrors() {
		t.Errorf("Failed parsing HCL test source")
	}
	testInput := hclFile.Body

	testCases := []struct {
		TestName    string
		DecoderOpts *DecoderOptions
		Target      interface{}
		Expected    interface{}
	}{
		{
			"nil_target",
			&DecoderOptions{},
			nil,
			nil,
		},
		{
			"decoder_opts_no_input",
			&DecoderOptions{},
			&testStruct{
				Test: "noChange",
			},
			&testStruct{
				Test: "noChange",
			},
		},
		{
			"decoder_opts_with_input",
			&DecoderOptions{
				Input: testInput,
			},
			&testStruct{},
			&testStruct{
				Test: "ateststring",
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.TestName, func(t *testing.T) {
			assert := assert.New(t)
			err := hclDecoder.Decode(tt.DecoderOpts, tt.Target)
			if err != nil {
				t.Errorf("decoding failed")
			}
			assert.Nil(err)

			if !reflect.DeepEqual(tt.Target, tt.Expected) {
				t.Errorf("GOT:\n%s\nEXPECTED:\n%s",
					spew.Sdump(tt.Target),
					spew.Sdump(tt.Expected))
			}
		})
	}
}

func TestCreateHclContext(t *testing.T) {
	t.Setenv("TEST_STRING", "ateststring")
	t.Setenv("TEST_INT", "2")
	type testHclStruct struct {
		TestStr string `hcl:"test_string"`
		TestInt int    `hcl:"test_int"`
	}

	evalCtx := CreateHclContext()
	hclDecoder := hclDecoder{evalCtx}
	hclSrc := `
test_string = env.TEST_STRING
test_int = env("TEST_INT")
`
	p := hclparse.NewParser()
	hclFile, diags := p.ParseHCL([]byte(hclSrc), "placeholder.hcl")
	if diags.HasErrors() {
		t.Errorf("Failed parsing HCL test source")
	}
	testInput := hclFile.Body

	testCases := []struct {
		TestName    string
		DecoderOpts *DecoderOptions
		Target      interface{}
		Expected    interface{}
	}{
		{
			"Hcl_eval_context_with_env_fun_and_var",
			&DecoderOptions{
				Input: testInput,
			},
			&testHclStruct{},
			&testHclStruct{
				TestStr: "ateststring",
				TestInt: 2,
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.TestName, func(t *testing.T) {
			assert := assert.New(t)

			err := hclDecoder.Decode(tt.DecoderOpts, tt.Target)
			if err != nil {
				t.Error(err.Error())
			}
			assert.Nil(err)

			if !reflect.DeepEqual(tt.Target, tt.Expected) {
				t.Errorf("GOT:\n%s\nEXPECTED:\n%s",
					spew.Sdump(tt.Target),
					spew.Sdump(tt.Expected))
			}
		})
	}
}
