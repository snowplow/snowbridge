/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

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
