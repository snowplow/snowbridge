//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package testutil

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/twinj/uuid"

	"github.com/snowplow/snowbridge/pkg/models"
)

const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var (
	seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))
)

// GenRandomString can produce a random string of any provided length which is
// useful for testing situations that might have byte limitations
func GenRandomString(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

// GetTestMessages will return an array of messages ready to be used for testing
// targets and sources
func GetTestMessages(count int, body string, ackFunc func()) []*models.Message {
	var messages []*models.Message
	for i := 0; i < count; i++ {
		messages = append(messages, &models.Message{
			Data:         []byte(body),
			PartitionKey: uuid.NewV4().String(),
			AckFunc:      ackFunc,
		})
	}
	return messages
}

// GetSequentialTestMessages will return an array of messages ready to be used for testing
// targets and sources. Message data will be sequential integers for easier testing of accuracy, duplicates, etc.
func GetSequentialTestMessages(count int, ackFunc func()) []*models.Message {
	var messages []*models.Message
	for i := 0; i < count; i++ {
		messages = append(messages, &models.Message{
			Data:         []byte(fmt.Sprint(i)),
			PartitionKey: uuid.NewV4().String(),
			AckFunc:      ackFunc,
		})
	}
	return messages
}
