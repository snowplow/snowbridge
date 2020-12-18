// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

import (
	"bufio"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/twinj/uuid"
	"os"
	"strings"
)

// StdinSource holds a new client for reading events from stdin
type StdinSource struct{}

// NewStdinSource creates a new client for reading events from stdin
func NewStdinSource() (*StdinSource, error) {
	return &StdinSource{}, nil
}

// Read will buffer all inputs until CTRL+D is pressed
func (ss *StdinSource) Read() ([]*Event, error) {
	var events []*Event

	fi, _ := os.Stdin.Stat()

	if (fi.Mode() & os.ModeCharDevice) == 0 {
		log.Info("Detected piped input, scanning until EOF detected (Note: Press 'CTRL + D' to exit)")

		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			events = append(events, &Event{
				Data:         []byte(scanner.Text()),
				PartitionKey: uuid.NewV4().String(),
			})
		}

		if scanner.Err() != nil {
			return nil, scanner.Err()
		}
	} else {
		reader := bufio.NewReader(os.Stdin)
		text, err := reader.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("Failed to read string from stdin: %s", err.Error())
		}

		trimmedText := strings.TrimSuffix(text, "\n")

		events = append(events, &Event{
			Data:         []byte(trimmedText),
			PartitionKey: uuid.NewV4().String(),
		})
	}

	return events, nil
}
