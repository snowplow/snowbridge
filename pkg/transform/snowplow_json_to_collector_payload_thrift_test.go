//
// Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package transform

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/pkg/models"
)

func TestSpJSONToCollectorPayloadThrift(t *testing.T) {
	assert := assert.New(t)

	var messageGood = models.Message{
		Data:         []byte(`{"ipAddress":"172.17.0.1","timestamp":1674428616042,"encoding":"UTF-8","collector":"ssc-2.8.2-stdout$","userAgent":"curl/7.85.0","path":"/i","querystring":"e=pv&p=aid","headers":["Timeout-Access: <function1>","Host: localhost:8080","User-Agent: curl/7.85.0","Accept: */*"],"hostname":"localhost","networkUserId":"5ffc81e1-7bf2-4084-80aa-f8874a53ce50","schema":"iglu:com.snowplowanalytics.snowplow/CollectorPayload/thrift/1-0-0"}`),
		PartitionKey: "some-key",
	}

	var messageBad = models.Message{
		Data:         []byte(`not-a-json`),
		PartitionKey: "some-key4",
	}

	var expectedGood = models.Message{
		Data:         []byte(`CwBkAAAACjE3Mi4xNy4wLjEKAMgAAAGF27kNagsA0gAAAAVVVEYtOAsA3AAAABFzc2MtMi44LjItc3Rkb3V0JAsBLAAAAAtjdXJsLzcuODUuMAsBQAAAAAIvaQsBSgAAAAplPXB2JnA9YWlkDwFeCwAAAAQAAAAbVGltZW91dC1BY2Nlc3M6IDxmdW5jdGlvbjE+AAAAFEhvc3Q6IGxvY2FsaG9zdDo4MDgwAAAAF1VzZXItQWdlbnQ6IGN1cmwvNy44NS4wAAAAC0FjY2VwdDogKi8qCwGQAAAACWxvY2FsaG9zdAsBmgAAACQ1ZmZjODFlMS03YmYyLTQwODQtODBhYS1mODg3NGE1M2NlNTALemkAAABBaWdsdTpjb20uc25vd3Bsb3dhbmFseXRpY3Muc25vd3Bsb3cvQ29sbGVjdG9yUGF5bG9hZC90aHJpZnQvMS0wLTAA`),
		PartitionKey: "some-key",
	}

	base64EncodeFunc, _ := NewSpJSONToCollectorPayloadThrift(true)

	// Simple success case
	transformSuccess, _, failure, _ := base64EncodeFunc(&messageGood, nil)

	assert.Equal(expectedGood.PartitionKey, transformSuccess.PartitionKey)
	assert.Equal(string(expectedGood.Data), string(transformSuccess.Data))
	assert.Nil(failure)

	// Simple failure case
	success, _, transformFailure, _ := base64EncodeFunc(&messageBad, nil)

	// Not matching equivalence of whole object because error stacktrace makes it unfeasible. Doing each component part instead.
	assert.NotNil(transformFailure.GetError())
	if transformFailure.GetError() != nil {
		assert.Equal("invalid character 'o' in literal null (expecting 'u')", transformFailure.GetError().Error())
	}
	assert.Equal([]byte("not-a-json"), transformFailure.Data)
	assert.Equal("some-key4", transformFailure.PartitionKey)
	assert.Nil(success)

	// Check that the input has not been altered
	assert.Nil(messageGood.GetError())
}
