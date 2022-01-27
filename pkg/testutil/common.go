// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package testutil

import (
	"math/rand"
	"time"

	"github.com/twinj/uuid"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
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
