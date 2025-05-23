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

package testutil

import (
	"fmt"
	"time"

	"github.com/snowplow/snowbridge/pkg/models"
	"github.com/snowplow/snowbridge/pkg/source/sourceiface"
)

// TODO: Refactor to provide a means to test errors without panicing

// ReadAndReturnMessages takes a source, runs the read function, and outputs all messages found in a slice, against which we may run assertions.
// The testWriteBuilder argument allows the test implementation to provide a write function builder,
// and the additionalOpts argument allows one to pass arguments to that builder
func ReadAndReturnMessages(source sourceiface.Source, timeToWait time.Duration, testWriteBuilder func(sourceiface.Source, chan *models.Message, any) func([]*models.Message) error, additionalOpts any) []*models.Message {
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
func DefaultTestWriteBuilder(source sourceiface.Source, msgChan chan *models.Message, additionalOpts any) func(messages []*models.Message) error {
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
func DelayedAckTestWriteBuilder(source sourceiface.Source, msgChan chan *models.Message, additionalOpts any) func(messages []*models.Message) error {
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
