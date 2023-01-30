//
// Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package collectorpayload

import (
	"context"
	"encoding/base64"
	"encoding/json"

	thrift "github.com/apache/thrift/lib/go/thrift"

	model1 "github.com/snowplow/snowbridge/third_party/snowplow/collectorpayload/gen-go/model1"
)

const (
	schema = "iglu:com.snowplowanalytics.snowplow/CollectorPayload/thrift/1-0-0"
)

// BinarySerializer serializes a CollectorPayload into a byte array ready for transport
func BinarySerializer(ctx context.Context, collectorPayload *model1.CollectorPayload) ([]byte, error) {
	collectorPayload.Schema = schema
	
	t := thrift.NewTMemoryBufferLen(1024)
	p := thrift.NewTBinaryProtocolFactoryDefault().GetProtocol(t)

	serializer := &thrift.TSerializer{
		Transport: t,
		Protocol:  p,
	}

	return serializer.Write(ctx, collectorPayload)
}

// BinaryDeserializer deserializes a CollectorPayload byte array back to a struct
func BinaryDeserializer(ctx context.Context, collectorPayloadBytes []byte) (*model1.CollectorPayload, error) {
	var inputBytes []byte

	// Attempt to decode from base64 as most payloads will arrive with the thrift string re-encoded
	base64DecodedCollectorPayload, base64Err := base64.StdEncoding.DecodeString(string(collectorPayloadBytes))
	if base64Err != nil {
		inputBytes = collectorPayloadBytes
	} else {
		inputBytes = []byte(base64DecodedCollectorPayload)
	}

	t := thrift.NewTMemoryBufferLen(1024)
	p := thrift.NewTBinaryProtocolFactoryDefault().GetProtocol(t)

	deserializer := &thrift.TDeserializer{
		Transport: t,
		Protocol:  p,
	}

	collectorPayload := model1.NewCollectorPayload()
	err := deserializer.Read(ctx, collectorPayload, inputBytes)

	collectorPayload.Schema = schema

	return collectorPayload, err
}

// ToJSON converts the collector payload struct to a JSON representation for simpler portability
func ToJSON(collectorPayload *model1.CollectorPayload) ([]byte, error) {
	return json.Marshal(collectorPayload)
}
