package testutil

import (
	"fmt"
	"time"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceiface"
)

// ReadAndReturnMessages takes a source, runs the read function, and outputs all messages found in a slice, against which we may run assertions.
func ReadAndReturnMessages(source sourceiface.Source) []*models.Message {
	var successfulReads []*models.Message

	hitError := make(chan error)
	msgRecieved := make(chan *models.Message)
	// run the read function in a goroutine, so that we can close it after a timeout
	sf := sourceiface.SourceFunctions{
		WriteToTarget: testWriteFuncBuilder(source, msgRecieved),
	}
	go runRead(hitError, source, &sf)

	for { // TODO: I think this pattern makes it threadsafe. Need to verify.
		select {
		case err1 := <-hitError:
			panic(err1)
		case msg := <-msgRecieved:
			successfulReads = append(successfulReads, msg)
		case <-time.After(3 * time.Second):
			// Stop source after 3s with no messages (should be ample time)
			fmt.Println("Stopping source.")
			source.Stop()
			return successfulReads
		}

	}

}

func runRead(ch chan error, source sourceiface.Source, sf *sourceiface.SourceFunctions) {
	err := source.Read(sf)
	if err != nil {
		ch <- err
	}
}

func testWriteFuncBuilder(source sourceiface.Source, msgChan chan *models.Message) func(messages []*models.Message) error {
	return func(messages []*models.Message) error {
		for _, msg := range messages {
			// Send each message onto the channel to be appended to results
			msgChan <- msg
			msg.AckFunc()
		}
		return nil
	}
}