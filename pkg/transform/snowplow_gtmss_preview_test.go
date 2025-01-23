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
package transform

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/pkg/models"
	"github.com/snowplow/snowplow-golang-analytics-sdk/analytics"
)

const fiftyYears = time.Hour * 24 * 365 * 50

func TestGTMSSPreview(t *testing.T) {
	testCases := []struct {
		Scenario        string
		Ctx             string
		Property        string
		HeaderKey       string
		Expiry          time.Duration
		InputMsg        *models.Message
		InputInterState interface{}
		Expected        map[string]*models.Message
		ExpInterState   interface{}
		Error           error
	}{
		{
			Scenario:  "main_case_with_gtmss",
			Ctx:       "contexts_com_google_tag-manager_server-side_preview_mode_1",
			Property:  "x-gtm-server-preview",
			HeaderKey: "x-gtm-server-preview",
			Expiry:    fiftyYears,
			InputMsg: &models.Message{
				Data:         spTsvWithGtmss,
				PartitionKey: "pk",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         spTsvWithGtmss,
					PartitionKey: "pk",
					HTTPHeaders: map[string]string{
						"x-gtm-server-preview": "ZW52LTcyN3wtMkMwR084ekptbWxiZmpkcHNIRENBfDE4ZTJkYzgxMDc2NDg1MjVmMzI2Mw==",
					},
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: spTsvWithGtmssParsed,
			Error:         nil,
		},
		{
			Scenario:  "main_case_no_gtmss",
			Ctx:       "contexts_com_google_tag-manager_server-side_preview_mode_1",
			Property:  "x-gtm-server-preview",
			HeaderKey: "x-gtm-server-preview",
			Expiry:    1 * time.Hour,
			InputMsg: &models.Message{
				Data:         spTsvNoGtmss,
				PartitionKey: "pk",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         spTsvNoGtmss,
					PartitionKey: "pk",
					HTTPHeaders:  nil,
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: spTsvNoGtmssParsed,
			Error:         nil,
		},
		{
			Scenario:  "non_snowplow_event",
			Ctx:       "contexts_com_google_tag-manager_server-side_preview_mode_1",
			Property:  "x-gtm-server-preview",
			HeaderKey: "x-gtm-server-preview",
			Expiry:    fiftyYears,
			InputMsg: &models.Message{
				Data:         []byte(`asdf`),
				PartitionKey: "pk",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success":  nil,
				"filtered": nil,
				"failed": {
					Data:         []byte(`asdf`),
					PartitionKey: "pk",
					HTTPHeaders:  nil,
				},
			},
			ExpInterState: nil,
			Error:         errors.New("parse"),
		},
		{
			Scenario:  "existing_headers_with_gtmss",
			Ctx:       "contexts_com_google_tag-manager_server-side_preview_mode_1",
			Property:  "x-gtm-server-preview",
			HeaderKey: "x-gtm-server-preview",
			Expiry:    fiftyYears,
			InputMsg: &models.Message{
				Data:         spTsvWithGtmss,
				PartitionKey: "pk",
				HTTPHeaders: map[string]string{
					"foo": "bar",
				},
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         spTsvWithGtmss,
					PartitionKey: "pk",
					HTTPHeaders: map[string]string{
						"foo":                  "bar",
						"x-gtm-server-preview": "ZW52LTcyN3wtMkMwR084ekptbWxiZmpkcHNIRENBfDE4ZTJkYzgxMDc2NDg1MjVmMzI2Mw==",
					},
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: spTsvWithGtmssParsed,
			Error:         nil,
		},
		{
			Scenario:  "existing_headers_append_with_gtmss",
			Ctx:       "contexts_com_google_tag-manager_server-side_preview_mode_1",
			Property:  "x-gtm-server-preview",
			HeaderKey: "x-gtm-server-preview",
			Expiry:    fiftyYears,
			InputMsg: &models.Message{
				Data:         spTsvWithGtmss,
				PartitionKey: "pk",
				HTTPHeaders: map[string]string{
					"foo":                  "bar",
					"x-gtm-server-preview": "existing",
				},
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         spTsvWithGtmss,
					PartitionKey: "pk",
					HTTPHeaders: map[string]string{
						"foo":                  "bar",
						"x-gtm-server-preview": "ZW52LTcyN3wtMkMwR084ekptbWxiZmpkcHNIRENBfDE4ZTJkYzgxMDc2NDg1MjVmMzI2Mw==",
					},
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: spTsvWithGtmssParsed,
			Error:         nil,
		},
		{
			Scenario:  "empty_headers_with_gtmss",
			Ctx:       "contexts_com_google_tag-manager_server-side_preview_mode_1",
			Property:  "x-gtm-server-preview",
			HeaderKey: "x-gtm-server-preview",
			Expiry:    fiftyYears,
			InputMsg: &models.Message{
				Data:         spTsvWithGtmss,
				PartitionKey: "pk",
				HTTPHeaders:  map[string]string{},
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         spTsvWithGtmss,
					PartitionKey: "pk",
					HTTPHeaders: map[string]string{
						"x-gtm-server-preview": "ZW52LTcyN3wtMkMwR084ekptbWxiZmpkcHNIRENBfDE4ZTJkYzgxMDc2NDg1MjVmMzI2Mw==",
					},
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: spTsvWithGtmssParsed,
			Error:         nil,
		},
		{
			Scenario:  "not_found_with_existing_headers",
			Ctx:       "app_id",
			Property:  "x-gtm-server-preview",
			HeaderKey: "x-gtm-server-preview",
			Expiry:    fiftyYears,
			InputMsg: &models.Message{
				Data:         spTsvWithGtmss,
				PartitionKey: "pk",
				HTTPHeaders:  map[string]string{"foo": "bar"},
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         spTsvWithGtmss,
					PartitionKey: "pk",
					HTTPHeaders: map[string]string{
						"foo": "bar",
					},
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: spTsvWithGtmssParsed,
			Error:         nil,
		},
		{
			Scenario:  "expired_message",
			Ctx:       "contexts_com_google_tag-manager_server-side_preview_mode_1",
			Property:  "x-gtm-server-preview",
			HeaderKey: "x-gtm-server-preview",
			Expiry:    1 * time.Hour,
			InputMsg: &models.Message{
				Data:         spTsvWithGtmss,
				PartitionKey: "pk",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success":  nil,
				"filtered": nil,
				"failed": {
					Data:         []byte(spTsvWithGtmss),
					PartitionKey: "pk",
					HTTPHeaders:  nil,
				},
			},
			ExpInterState: nil,
			Error:         errors.New("Message has expired"),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Scenario, func(t *testing.T) {
			transFunction := gtmssPreviewTransformation(tt.Ctx, tt.Property, tt.HeaderKey, tt.Expiry)
			s, f, e, i := transFunction(tt.InputMsg, tt.InputInterState)

			if !reflect.DeepEqual(i, tt.ExpInterState) {
				t.Errorf("\nINTERMEDIATE_STATE:\nGOT:\n%s\nEXPECTED:\n%s",
					spew.Sdump(i),
					spew.Sdump(tt.ExpInterState))
			}

			if e == nil && tt.Error != nil {
				t.Fatalf("missed expected error")
			}

			if e != nil {
				gotErr := e.GetError()
				expErr := tt.Error
				if expErr == nil {
					t.Fatalf("got unexpected error: %s", gotErr.Error())
				}

				if !strings.Contains(gotErr.Error(), expErr.Error()) {
					t.Errorf("GOT_ERROR:\n%s\n does not contain\nEXPECTED_ERROR:\n%s",
						gotErr.Error(),
						expErr.Error())
				}
			}

			assertMessagesCompareGtmss(t, s, tt.Expected["success"], "success")
			assertMessagesCompareGtmss(t, f, tt.Expected["filtered"], "filtered")
			assertMessagesCompareGtmss(t, e, tt.Expected["failed"], "failed")
		})
	}
}

func TestExtractHeaderValue(t *testing.T) {
	expectedValue := "ZW52LTcyN3wtMkMwR084ekptbWxiZmpkcHNIRENBfDE4ZTJkYzgxMDc2NDg1MjVmMzI2Mw=="
	testCases := []struct {
		Scenario string
		Event    analytics.ParsedEvent
		Ctx      string
		Prop     string
		Expected *string
		Error    error
	}{
		{
			Scenario: "happy_path",
			Event:    spTsvWithGtmssParsed,
			Ctx:      "contexts_com_google_tag-manager_server-side_preview_mode_1",
			Prop:     "x-gtm-server-preview",
			Expected: &expectedValue,
			Error:    nil,
		},
		{
			Scenario: "ctx_not_in_event",
			Event:    spTsvNoGtmssParsed,
			Ctx:      "contexts_com_google_tag-manager_server-side_preview_mode_1",
			Prop:     "x-gtm-server-preview",
			Expected: nil,
			Error:    nil,
		},
		{
			Scenario: "toMap_fails",
			Event:    analytics.ParsedEvent{"f", "a", "i", "l"},
			Ctx:      "foo",
			Prop:     "bar",
			Expected: nil,
			Error:    errors.New("wrong number of fields"),
		},
		{
			Scenario: "not_a_context_same_as_no_such_context",
			Event:    spTsvNoGtmssParsed,
			Ctx:      "app_id",
			Prop:     "foobar",
			Expected: nil,
			Error:    nil,
		},
		{
			Scenario: "invalid_header_value (not a string type)",
			Event:    fakeSpTsvParsed,
			Ctx:      "contexts_com_snowplowanalytics_snowplow_web_page_1",
			Prop:     "id",
			Expected: nil,
			Error:    errors.New("invalid header value"),
		},
		{
			Scenario: "invalid_header_value (not base64 encoding)",
			Event:    gtmssInvalidNoB64Parsed,
			Ctx:      "contexts_com_google_tag-manager_server-side_preview_mode_1",
			Prop:     "x-gtm-server-preview",
			Expected: nil,
			Error:    errors.New("illegal base64 data at input"),
		},
		{
			Scenario: "event_without_contexts",
			Event:    spTsvNoCtxParsed,
			Ctx:      "contexts_com_snowplowanalytics_snowplow_web_page_1",
			Prop:     "id",
			Expected: nil,
			Error:    nil,
		},
		{
			Scenario: "missing_property",
			Event:    fakeSpTsvParsed,
			Ctx:      "contexts_com_snowplowanalytics_snowplow_web_page_1",
			Prop:     "doesNotExist",
			Expected: nil,
			Error:    nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Scenario, func(t *testing.T) {
			assert := assert.New(t)
			result, err := extractHeaderValue(tt.Event, tt.Ctx, tt.Prop)
			if err == nil && tt.Error != nil {
				t.Fatalf("missed expected error")
			}

			if err != nil {
				expErr := tt.Error
				if expErr == nil {
					t.Fatalf("got unexpected error: %s", err.Error())
				}

				if !strings.Contains(err.Error(), expErr.Error()) {
					t.Errorf("GOT_ERROR:\n%s\n does not contain\nEXPECTED_ERROR:\n%s",
						err.Error(),
						expErr.Error())
				}
			}

			assert.Equal(result, tt.Expected)
		})
	}
}

func Benchmark_GTMSSPreview_With_Preview_Ctx_no_intermediate(b *testing.B) {
	b.ReportAllocs()

	inputMsg := &models.Message{
		Data:         spTsvWithGtmss,
		PartitionKey: "pk",
	}
	ctx := "contexts_com_google_tag-manager_server-side_preview_mode_1"
	prop := "x-gtm-server-preview"
	header := "x-gtm-server-preview"

	transFunction := gtmssPreviewTransformation(ctx, prop, header, fiftyYears)

	for n := 0; n < b.N; n++ {
		transFunction(inputMsg, nil)
	}
}

func Benchmark_GTMSSPreview_With_Preview_Ctx_With_intermediate(b *testing.B) {
	b.ReportAllocs()

	inputMsg := &models.Message{
		Data:         spTsvWithGtmss,
		PartitionKey: "pk",
	}
	interState := spTsvWithGtmssParsed
	ctx := "contexts_com_google_tag-manager_server-side_preview_mode_1"
	prop := "x-gtm-server-preview"
	header := "x-gtm-server-preview"

	transFunction := gtmssPreviewTransformation(ctx, prop, header, fiftyYears)

	for n := 0; n < b.N; n++ {
		transFunction(inputMsg, interState)
	}
}

func Benchmark_GTMSSPreview_No_Preview_Ctx_no_intermediate(b *testing.B) {
	b.ReportAllocs()

	inputMsg := &models.Message{
		Data:         spTsvNoGtmss,
		PartitionKey: "pk",
	}
	ctx := "contexts_com_google_tag-manager_server-side_preview_mode_1"
	prop := "x-gtm-server-preview"
	header := "x-gtm-server-preview"

	transFunction := gtmssPreviewTransformation(ctx, prop, header, fiftyYears)

	for n := 0; n < b.N; n++ {
		transFunction(inputMsg, nil)
	}
}

func Benchmark_GTMSSPreview_No_Preview_Ctx_With_intermediate(b *testing.B) {
	b.ReportAllocs()

	inputMsg := &models.Message{
		Data:         spTsvNoGtmss,
		PartitionKey: "pk",
	}
	interState := spTsvNoGtmssParsed
	ctx := "contexts_com_google_tag-manager_server-side_preview_mode_1"
	prop := "x-gtm-server-preview"
	header := "x-gtm-server-preview"

	transFunction := gtmssPreviewTransformation(ctx, prop, header, fiftyYears)

	for n := 0; n < b.N; n++ {
		transFunction(inputMsg, interState)
	}
}

func assertMessagesCompareGtmss(t *testing.T, act, exp *models.Message, hint string) {
	t.Helper()
	ok := false
	headersOk := false
	switch {
	case act == nil:
		ok = exp == nil
	case exp == nil:
	default:
		pkOk := act.PartitionKey == exp.PartitionKey
		dataOk := reflect.DeepEqual(act.Data, exp.Data)
		cTimeOk := reflect.DeepEqual(act.TimeCreated, exp.TimeCreated)
		pTimeOk := reflect.DeepEqual(act.TimePulled, exp.TimePulled)
		tTimeOk := reflect.DeepEqual(act.TimeTransformed, exp.TimeTransformed)
		ackOk := reflect.DeepEqual(act.AckFunc, exp.AckFunc)
		headersOk = reflect.DeepEqual(act.HTTPHeaders, exp.HTTPHeaders)

		if pkOk && dataOk && cTimeOk && pTimeOk && tTimeOk && ackOk && headersOk {
			ok = true
		}
	}

	if !ok {
		// message.HTTPHeaders are not printed
		if headersOk == false {
			t.Errorf("\nHTTPHeaders DIFFER:\nGOT:\n%s\nEXPECTED:\n%s\n",
				spew.Sdump(act.HTTPHeaders),
				spew.Sdump(exp.HTTPHeaders))
		} else {
			t.Errorf("MESSAGES DIFFER\nGOT:\n%s\nEXPECTED[%s]:\n%s\n",
				spew.Sdump(act),
				hint,
				spew.Sdump(exp))
		}
	}
}

var spTsvNoGtmss = []byte(`media-test	web	2024-03-12 04:25:40.277	2024-03-12 04:25:40.272	2024-03-12 04:25:36.685	page_view	1313411b-282f-4aa9-b37c-c60d4723cf47		spTest	js-3.17.0	snowplow-micro-2.0.0-stdout$	snowplow-micro-2.0.0	media_tester	172.17.0.1		23a0eb65-83f6-4957-839e-f3044bfefb99	1	a2f53212-26a3-4781-81d6-f14aa8d4552b												http://localhost:8000/	Test Media Tracking		http	localhost	8000	/																	{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0","data":{"id":"763c8939-4694-412a-b96c-32cd76b9ddb2"}},{"schema":"iglu:com.google.tag-manager.server-side/user_data/jsonschema/1-0-0","data":{"email_address":"foo@example.com","phone_number":"+15551234567","address":{"first_name":"Jane","last_name":"Doe","street":"123 Fake St","city":"San Francisco","region":"CA","postal_code":"94016","country":"US"}}},{"schema":"iglu:com.snowplowanalytics.snowplow/mobile_context/jsonschema/1-0-2","data":{"osType":"testOsType","osVersion":"testOsVersion","deviceManufacturer":"testDevMan","deviceModel":"testDevModel"}},{"schema":"iglu:com.snowplowanalytics.snowplow/client_session/jsonschema/1-0-2","data":{"userId":"23a0eb65-83f6-4957-839e-f3044bfefb99","sessionId":"73fcdaa3-0164-41ce-a336-fb00c4ebf68c","eventIndex":2,"sessionIndex":1,"previousSessionId":null,"storageMechanism":"COOKIE_1","firstEventId":"327b9ff9-ed5f-40cf-918a-1b1a775ae347","firstEventTimestamp":"2024-03-12T04:25:36.684Z"}}]}																									Mozilla/5.0 (X11; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0						en-US										1	24	1920	935				Europe/Athens			1920	1080	windows-1252	1920	935												2024-03-12 04:25:40.268				73fcdaa3-0164-41ce-a336-fb00c4ebf68c	2024-03-12 04:25:36.689	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0		`)
var spTsvNoGtmssParsed, _ = analytics.ParseEvent(string(spTsvNoGtmss))

var spTsvWithGtmss = []byte(`media-test	web	2024-03-12 04:27:01.760	2024-03-12 04:27:01.755	2024-03-12 04:27:01.743	unstruct	9be3afe8-8a62-41ac-93db-12f425d82ac9		spTest	js-3.17.0	snowplow-micro-2.0.0-stdout$	snowplow-micro-2.0.0	media_tester	172.17.0.1		23a0eb65-83f6-4957-839e-f3044bfefb99	1	a2f53212-26a3-4781-81d6-f14aa8d4552b												http://localhost:8000/?sgtm-preview-header=ZW52LTcyN3wtMkMwR084ekptbWxiZmpkcHNIRENBfDE4ZTJkYzgxMDc2NDg1MjVmMzI2Mw==			http	localhost	8000	/	sgtm-preview-header=ZW52LTcyN3wtMkMwR084ekptbWxiZmpkcHNIRENBfDE4ZTJkYzgxMDc2NDg1MjVmMzI2Mw==																{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[{"schema":"iglu:org.whatwg/media_element/jsonschema/1-0-0","data":{"htmlId":"bunny-mp4","mediaType":"VIDEO","autoPlay":false,"buffered":[{"start":0,"end":1.291666}],"controls":true,"currentSrc":"https://archive.org/download/BigBuckBunny_124/Content/big_buck_bunny_720p_surround.mp4","defaultMuted":false,"defaultPlaybackRate":1,"error":null,"networkState":"NETWORK_LOADING","preload":"","readyState":"HAVE_ENOUGH_DATA","seekable":[{"start":0,"end":596.503219}],"seeking":false,"src":"https://archive.org/download/BigBuckBunny_124/Content/big_buck_bunny_720p_surround.mp4","textTracks":[],"fileExtension":"mp4","fullscreen":false,"pictureInPicture":false}},{"schema":"iglu:com.snowplowanalytics.snowplow/media_player/jsonschema/1-0-0","data":{"currentTime":0,"duration":596.503219,"ended":false,"loop":false,"muted":false,"paused":false,"playbackRate":1,"volume":100}},{"schema":"iglu:org.whatwg/video_element/jsonschema/1-0-0","data":{"poster":"","videoHeight":360,"videoWidth":640}},{"schema":"iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0","data":{"id":"021c4d09-e502-4562-8182-5ac7247125ec"}},{"schema":"iglu:com.google.tag-manager.server-side/user_data/jsonschema/1-0-0","data":{"email_address":"foo@example.com","phone_number":"+15551234567","address":{"first_name":"Jane","last_name":"Doe","street":"123 Fake St","city":"San Francisco","region":"CA","postal_code":"94016","country":"US"}}},{"schema":"iglu:com.snowplowanalytics.snowplow/mobile_context/jsonschema/1-0-2","data":{"osType":"testOsType","osVersion":"testOsVersion","deviceManufacturer":"testDevMan","deviceModel":"testDevModel"}},{"schema":"iglu:com.google.tag-manager.server-side/preview_mode/jsonschema/1-0-0","data":{"x-gtm-server-preview":"ZW52LTcyN3wtMkMwR084ekptbWxiZmpkcHNIRENBfDE4ZTJkYzgxMDc2NDg1MjVmMzI2Mw=="}},{"schema":"iglu:com.snowplowanalytics.snowplow/client_session/jsonschema/1-0-2","data":{"userId":"23a0eb65-83f6-4957-839e-f3044bfefb99","sessionId":"73fcdaa3-0164-41ce-a336-fb00c4ebf68c","eventIndex":7,"sessionIndex":1,"previousSessionId":null,"storageMechanism":"COOKIE_1","firstEventId":"327b9ff9-ed5f-40cf-918a-1b1a775ae347","firstEventTimestamp":"2024-03-12T04:25:36.684Z"}}]}						{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.snowplowanalytics.snowplow/media_player_event/jsonschema/1-0-0","data":{"type":"play"}}}																			Mozilla/5.0 (X11; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0						en-US										1	24	1920	935				Europe/Athens			1920	1080	windows-1252	1920	935												2024-03-12 04:27:01.745				73fcdaa3-0164-41ce-a336-fb00c4ebf68c	2024-03-12 04:27:01.753	com.snowplowanalytics.snowplow	media_player_event	jsonschema	1-0-0		`)
var spTsvWithGtmssParsed, _ = analytics.ParseEvent(string(spTsvWithGtmss))

var gtmssInvalidNoB64 = []byte(`media-test	web	2024-03-12 04:27:01.760	2024-03-12 04:27:01.755	2024-03-12 04:27:01.743	unstruct	9be3afe8-8a62-41ac-93db-12f425d82ac9		spTest	js-3.17.0	snowplow-micro-2.0.0-stdout$	snowplow-micro-2.0.0	media_tester	172.17.0.1		23a0eb65-83f6-4957-839e-f3044bfefb99	1	a2f53212-26a3-4781-81d6-f14aa8d4552b												http://localhost:8000/?sgtm-preview-header=ZW52LTcyN3wtMkMwR084ekptbWxiZmpkcHNIRENBfDE4ZTJkYzgxMDc2NDg1MjVmMzI2Mw==			http	localhost	8000	/	sgtm-preview-header=ZW52LTcyN3wtMkMwR084ekptbWxiZmpkcHNIRENBfDE4ZTJkYzgxMDc2NDg1MjVmMzI2Mw==																{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[{"schema":"iglu:org.whatwg/media_element/jsonschema/1-0-0","data":{"htmlId":"bunny-mp4","mediaType":"VIDEO","autoPlay":false,"buffered":[{"start":0,"end":1.291666}],"controls":true,"currentSrc":"https://archive.org/download/BigBuckBunny_124/Content/big_buck_bunny_720p_surround.mp4","defaultMuted":false,"defaultPlaybackRate":1,"error":null,"networkState":"NETWORK_LOADING","preload":"","readyState":"HAVE_ENOUGH_DATA","seekable":[{"start":0,"end":596.503219}],"seeking":false,"src":"https://archive.org/download/BigBuckBunny_124/Content/big_buck_bunny_720p_surround.mp4","textTracks":[],"fileExtension":"mp4","fullscreen":false,"pictureInPicture":false}},{"schema":"iglu:com.snowplowanalytics.snowplow/media_player/jsonschema/1-0-0","data":{"currentTime":0,"duration":596.503219,"ended":false,"loop":false,"muted":false,"paused":false,"playbackRate":1,"volume":100}},{"schema":"iglu:org.whatwg/video_element/jsonschema/1-0-0","data":{"poster":"","videoHeight":360,"videoWidth":640}},{"schema":"iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0","data":{"id":"021c4d09-e502-4562-8182-5ac7247125ec"}},{"schema":"iglu:com.google.tag-manager.server-side/user_data/jsonschema/1-0-0","data":{"email_address":"foo@example.com","phone_number":"+15551234567","address":{"first_name":"Jane","last_name":"Doe","street":"123 Fake St","city":"San Francisco","region":"CA","postal_code":"94016","country":"US"}}},{"schema":"iglu:com.snowplowanalytics.snowplow/mobile_context/jsonschema/1-0-2","data":{"osType":"testOsType","osVersion":"testOsVersion","deviceManufacturer":"testDevMan","deviceModel":"testDevModel"}},{"schema":"iglu:com.google.tag-manager.server-side/preview_mode/jsonschema/1-0-0","data":{"x-gtm-server-preview":"this is not valid base64"}},{"schema":"iglu:com.snowplowanalytics.snowplow/client_session/jsonschema/1-0-2","data":{"userId":"23a0eb65-83f6-4957-839e-f3044bfefb99","sessionId":"73fcdaa3-0164-41ce-a336-fb00c4ebf68c","eventIndex":7,"sessionIndex":1,"previousSessionId":null,"storageMechanism":"COOKIE_1","firstEventId":"327b9ff9-ed5f-40cf-918a-1b1a775ae347","firstEventTimestamp":"2024-03-12T04:25:36.684Z"}}]}						{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.snowplowanalytics.snowplow/media_player_event/jsonschema/1-0-0","data":{"type":"play"}}}																			Mozilla/5.0 (X11; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0						en-US										1	24	1920	935				Europe/Athens			1920	1080	windows-1252	1920	935												2024-03-12 04:27:01.745				73fcdaa3-0164-41ce-a336-fb00c4ebf68c	2024-03-12 04:27:01.753	com.snowplowanalytics.snowplow	media_player_event	jsonschema	1-0-0		`)
var gtmssInvalidNoB64Parsed, _ = analytics.ParseEvent(string(gtmssInvalidNoB64))

var fakeSpTsv = []byte(`media-test	web	2024-03-12 04:25:40.277	2024-03-12 04:25:40.272	2024-03-12 04:25:36.685	page_view	1313411b-282f-4aa9-b37c-c60d4723cf47		spTest	js-3.17.0	snowplow-micro-2.0.0-stdout$	snowplow-micro-2.0.0	media_tester	172.17.0.1		23a0eb65-83f6-4957-839e-f3044bfefb99	1	a2f53212-26a3-4781-81d6-f14aa8d4552b												http://localhost:8000/	Test Media Tracking		http	localhost	8000	/																	{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0","data":{"id":["FAILS"]}},{"schema":"iglu:com.google.tag-manager.server-side/user_data/jsonschema/1-0-0","data":{"email_address":"foo@example.com","phone_number":"+15551234567","address":{"first_name":"Jane","last_name":"Doe","street":"123 Fake St","city":"San Francisco","region":"CA","postal_code":"94016","country":"US"}}},{"schema":"iglu:com.snowplowanalytics.snowplow/mobile_context/jsonschema/1-0-2","data":{"osType":"testOsType","osVersion":"testOsVersion","deviceManufacturer":"testDevMan","deviceModel":"testDevModel"}},{"schema":"iglu:com.snowplowanalytics.snowplow/client_session/jsonschema/1-0-2","data":{"userId":"23a0eb65-83f6-4957-839e-f3044bfefb99","sessionId":"73fcdaa3-0164-41ce-a336-fb00c4ebf68c","eventIndex":2,"sessionIndex":1,"previousSessionId":null,"storageMechanism":"COOKIE_1","firstEventId":"327b9ff9-ed5f-40cf-918a-1b1a775ae347","firstEventTimestamp":"2024-03-12T04:25:36.684Z"}}]}																									Mozilla/5.0 (X11; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0						en-US										1	24	1920	935				Europe/Athens			1920	1080	windows-1252	1920	935												2024-03-12 04:25:40.268				73fcdaa3-0164-41ce-a336-fb00c4ebf68c	2024-03-12 04:25:36.689	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0		`)
var fakeSpTsvParsed, _ = analytics.ParseEvent(string(fakeSpTsv))

var spTsvNoCtx = []byte(`media-test	web	2024-03-12 04:27:01.760	2024-03-12 04:27:01.755	2024-03-12 04:27:01.743	unstruct	9be3afe8-8a62-41ac-93db-12f425d82ac9		spTest	js-3.17.0	snowplow-micro-2.0.0-stdout$	snowplow-micro-2.0.0	media_tester	172.17.0.1		23a0eb65-83f6-4957-839e-f3044bfefb99	1	a2f53212-26a3-4781-81d6-f14aa8d4552b												http://localhost:8000/?sgtm-preview-header=ZW52LTcyN3wtMkMwR084ekptbWxiZmpkcHNIRENBfDE4ZTJkYzgxMDc2NDg1MjVmMzI2Mw==			http	localhost	8000	/	sgtm-preview-header=ZW52LTcyN3wtMkMwR084ekptbWxiZmpkcHNIRENBfDE4ZTJkYzgxMDc2NDg1MjVmMzI2Mw==																						{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.snowplowanalytics.snowplow/media_player_event/jsonschema/1-0-0","data":{"type":"play"}}}																			Mozilla/5.0 (X11; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0						en-US										1	24	1920	935				Europe/Athens			1920	1080	windows-1252	1920	935												2024-03-12 04:27:01.745				73fcdaa3-0164-41ce-a336-fb00c4ebf68c	2024-03-12 04:27:01.753	com.snowplowanalytics.snowplow	media_player_event	jsonschema	1-0-0		`)

var spTsvNoCtxParsed, _ = analytics.ParseEvent(string(spTsvNoCtx))
