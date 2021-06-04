// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package transform

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"io"
	"os/exec"
	"sync"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
)

// writeNextB64Message will convert a byte array to a string, base-64 encode the payload and
// then append a newline to the message to signal the end of input to receiving pipe.
func writeNextB64Message(pipe io.WriteCloser, messageData []byte, logger *log.Entry) {
	encodedString := base64.StdEncoding.EncodeToString(messageData)
	logger.Debugf("Source: %s; Encoded: %s", string(messageData), encodedString)

	_, err := io.WriteString(pipe, fmt.Sprintf("%s\n", encodedString))
	if err != nil {
		logger.Fatal(errors.Wrap(err, "Error writing string to destination pipe"))
	}
}

// readNextB64Message will wait for a newline delimited input to arrive on the pipe and
// will then base64 decode the message and return it.
func readNextB64Message(pipe io.ReadCloser, logger *log.Entry) string {
	scanner := bufio.NewScanner(pipe)

	var str string
	for scanner.Scan() {
		var encodedStr = scanner.Text()
		originalStringBytes, err := base64.StdEncoding.DecodeString(encodedStr)
		if err != nil {
			logger.Fatal(errors.Wrap(err, "Error occured during base64 decode"))
		}
		str = string(originalStringBytes)
		break
	}

	if err := scanner.Err(); err != nil {
		logger.Fatal(errors.Wrap(err, "Error occurred scanning for next input"))
	}

	return str
}

// NewPluginFunction returns a TransformationFunction which will launch a process
// for every message passed to it.
//
// All messages are base64 encoded on send and base64 decoded on retrieval.  The calling function
// must mimic this behaviour.
//
// WARNING: Performance is very poort due to overhead of opening files for every message
//          being processed in this fashion.
func NewPluginFunction(pluginPath string) TransformationFunction {
	logger := log.WithFields(log.Fields{"transform": "plugin", "path": pluginPath})

	// TODO: Handle intermediateState pass-through
	return func(message *models.Message, intermediateState interface{}) (*models.Message, *models.Message, interface{}) {
		cmd := exec.Command(pluginPath)

		stdinPipe, err := cmd.StdinPipe()
		if err != nil {
			logger.Fatal(err.Error())
		}
		stdoutPipe, err := cmd.StdoutPipe()
		if err != nil {
			logger.Fatal(err.Error())
		}
		defer stdoutPipe.Close()
		stderrPipe, err := cmd.StderrPipe()
		if err != nil {
			logger.Fatal(err.Error())
		}
		defer stderrPipe.Close()

		go func() {
			defer stdinPipe.Close()
			writeNextB64Message(stdinPipe, message.Data, logger)
		}()

		err = cmd.Start()
		if err != nil {
			logger.Fatal(err.Error())
		}
		logger.Debugf("Running subprocess %d ...", cmd.Process.Pid)

		stdoutStr := readNextB64Message(stdoutPipe, logger)
		logger.Debugf("Stdout (%d): %s", cmd.Process.Pid, stdoutStr)
		stderrStr := readNextB64Message(stderrPipe, logger)
		logger.Debugf("Stderr (%d): %s", cmd.Process.Pid, stderrStr)

		err = cmd.Wait()
		if err != nil {
			logger.Fatal(err.Error())
		}

		if len(stderrStr) > 0 {
			message.SetError(errors.New(stderrStr))
			return nil, message, intermediateState
		}

		message.Data = []byte(stdoutStr)
		return message, nil, intermediateState
	}
}

// NewPluginLongFunction returns a TransformationFunction which launches a long running
// process to transform all data that is sent through it.
//
// All messages are base64 encoded on send and base64 decoded on retrieval.  The calling function
// must mimic this behaviour.
func NewPluginLongFunction(pluginPath string) TransformationFunction {
	logger := log.WithFields(log.Fields{"transform": "pluginLong", "path": pluginPath})

	cmd := exec.Command(pluginPath)

	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		logger.Fatal(err.Error())
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		logger.Fatal(err.Error())
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		logger.Fatal(err.Error())
	}

	err = cmd.Start()
	if err != nil {
		logger.Fatal(err.Error())
	}
	logger.Debugf("Running subprocess %d ...", cmd.Process.Pid)

	var m sync.Mutex

	// TODO: Handle intermediateState pass-through
	return func(message *models.Message, intermediateState interface{}) (*models.Message, *models.Message, interface{}) {
		m.Lock()

		writeNextB64Message(stdinPipe, message.Data, logger)
		stdoutStr := readNextB64Message(stdoutPipe, logger)
		logger.Debugf("Stdout (%d): %s", cmd.Process.Pid, stdoutStr)
		stderrStr := readNextB64Message(stderrPipe, logger)
		logger.Debugf("Stderr (%d): %s", cmd.Process.Pid, stderrStr)

		m.Unlock()

		if len(stderrStr) > 0 {
			message.SetError(errors.New(stderrStr))
			return nil, message, intermediateState
		}
		
		message.Data = []byte(stdoutStr)
		return message, nil, intermediateState
	}
}
