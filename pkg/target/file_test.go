package target

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/snowplow/snowbridge/pkg/testutil"
	"github.com/stretchr/testify/assert"
)

func TestFileTarget_WriteSuccess(t *testing.T) {
	assert := assert.New(t)

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.txt")

	config := &FileTargetConfig{
		Path: tmpFile,
	}

	target, err := newFileTarget(config)
	assert.NotNil(target)
	assert.Nil(err)
	assert.Equal("file", target.GetID())

	target.Open()
	defer target.Close()

	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetTestMessages(1, "Hello World!", ackFunc)

	writeRes, err := target.Write(messages)
	assert.Nil(err)
	assert.NotNil(writeRes)

	// Check that Ack is called
	assert.Equal(int64(1), ackOps)

	// Check results
	assert.Equal(int64(1), writeRes.SentCount)
	assert.Equal(int64(0), writeRes.FailedCount)
	assert.Equal(0, len(writeRes.Oversized))

	// Check file contents
	content, err := os.ReadFile(tmpFile)
	assert.Nil(err)
	assert.Equal("Hello World!\n", string(content))
}
