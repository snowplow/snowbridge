// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"os"
	"io/ioutil"
)

func TestStdinSource_ReadSuccess(t *testing.T) {
	assert := assert.New(t)

	// Setup test input
	content := []byte("Hello World!")
	tmpfile, err := ioutil.TempFile("", "example")
	assert.Nil(err)
	defer os.Remove(tmpfile.Name())

	_, err = tmpfile.Write(content)
	assert.Nil(err)
	_, err = tmpfile.Seek(0, 0)
	assert.Nil(err)

	oldStdin := os.Stdin
	defer func() { os.Stdin = oldStdin }()
	os.Stdin = tmpfile

	// Read from test input
	source, err := NewStdinSource()
	assert.NotNil(source)
	assert.Nil(err)

	writeFunc := func(events []*Event) error {
		for _, event := range events {
			assert.Equal("Hello World!", string(event.Data))
		}
		return nil
	}
	closeFunc := func() {}

	sf := SourceFunctions{
		WriteToTarget: writeFunc,
		CloseTarget:   closeFunc,
	}

	err1 := source.Read(&sf)
	assert.Nil(err1)
}
