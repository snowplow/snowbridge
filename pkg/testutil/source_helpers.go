// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package testutil

import (
	"fmt"
	"time"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceiface"
)

// TODO: Refactor to provide a means to test errors without panicing

// ReadAndReturnMessages takes a source, runs the read function, and outputs all messages found in a slice, against which we may run assertions.
// The testWriteBuilder argument allows the test implementation to provide a write function builder,
// and the additionalOpts argument allows one to pass arguments to that builder
func ReadAndReturnMessages(source sourceiface.Source, timeToWait time.Duration, testWriteBuilder func(sourceiface.Source, chan *models.Message, interface{}) func([]*models.Message) error, additionalOpts interface{}) []*models.Message {
	var successfulReads []*models.Message

	hitError := make(chan error)
	msgRecieved := make(chan *models.Message)
	// run the read function in a goroutine, so that we can close it after a timeout
	sf := sourceiface.SourceFunctions{
		WriteToTarget: testWriteBuilder(source, msgRecieved, additionalOpts),
	}
	go runRead(hitError, source, &sf)

resultLoop:
	for {
		select {
		case err1 := <-hitError:
			panic(err1)
		case msg := <-msgRecieved:
			// Append messages to the result slice
			successfulReads = append(successfulReads, msg)
		case <-time.After(timeToWait):
			// Stop source after 3s, and return the result slice
			fmt.Println("Stopping source.")
			source.Stop()
			break resultLoop
		}
	}
	return successfulReads
}

func runRead(ch chan error, source sourceiface.Source, sf *sourceiface.SourceFunctions) {
	err := source.Read(sf)
	if err != nil {
		ch <- err
	}
}

// DefaultTestWriteBuilder returns a function which replaces the write function, outputting any messages it finds to be handled via a channel
func DefaultTestWriteBuilder(source sourceiface.Source, msgChan chan *models.Message, additionalOpts interface{}) func(messages []*models.Message) error {
	return func(messages []*models.Message) error {
		for _, msg := range messages {
			// Send each message onto the channel to be appended to results
			msgChan <- msg
			msg.AckFunc()
		}
		return nil
	}
}

// DelayedAckTestWriteBuilder delays every third ack, to test the case where some messages are processed slower than others
func DelayedAckTestWriteBuilder(source sourceiface.Source, msgChan chan *models.Message, additionalOpts interface{}) func(messages []*models.Message) error {
	return func(messages []*models.Message) error {
		duration, ok := additionalOpts.(time.Duration)
		if !ok {
			panic("DelayedAckTestWriteBuilder requires a duration to be passed as additionalOpts")
		}
		for i, msg := range messages {
			// Send each message onto the channel to be appended to results
			msgChan <- msg
			if i%3 == 1 {
				time.Sleep(duration)
			}
			msg.AckFunc()
		}
		return nil
	}
}
