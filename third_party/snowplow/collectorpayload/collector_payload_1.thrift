//
// Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

namespace go model1

struct CollectorPayload {
  31337: string schema

  // Required fields which are intrinsic properties of HTTP
  100: string ipAddress

  // Required fields which are Snowplow-specific
  200: i64 timestamp
  210: string encoding
  220: string collector

  // Optional fields which are intrinsic properties of HTTP
  300: optional string userAgent
  310: optional string refererUri
  320: optional string path
  330: optional string querystring
  340: optional string body
  350: optional list<string> headers
  360: optional string contentType

  // Optional fields which are Snowplow-specific
  400: optional string hostname
  410: optional string networkUserId
}
