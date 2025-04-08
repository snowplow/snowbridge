package target

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	log "github.com/sirupsen/logrus"
	"github.com/snowplow/snowbridge/pkg/models"
)

// FileTargetConfig configures the file output
type FileTargetConfig struct {
	Path        string `hcl:"path"`
	Permissions string `hcl:"permissions,optional"`
	Append      bool   `hcl:"append,optional"`
	MaxSize     int64  `hcl:"max_size,optional"`
	MaxBackups  int    `hcl:"max_backups,optional"`
}

// FileTarget implements Target interface for file output
type FileTarget struct {
	file        *os.File
	path        string
	permissions os.FileMode
	append      bool
	maxSize     int64
	maxBackups  int
	log         *log.Entry
}

func newFileTarget(config *FileTargetConfig) (*FileTarget, error) {
	if config.Path == "" {
		return nil, fmt.Errorf("path is required for file target")
	}

	permissions := os.FileMode(0644)
	if config.Permissions != "" {
		perm, err := strconv.ParseUint(config.Permissions, 8, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid permissions: %s", err)
		}
		permissions = os.FileMode(perm)
	}

	maxSize := int64(100 * 1024 * 1024) // 100MB default
	if config.MaxSize > 0 {
		maxSize = config.MaxSize
	}

	maxBackups := 3
	if config.MaxBackups > 0 {
		maxBackups = config.MaxBackups
	}

	return &FileTarget{
		path:        config.Path,
		permissions: permissions,
		append:      config.Append,
		maxSize:     maxSize,
		maxBackups:  maxBackups,
		log:         log.WithFields(log.Fields{"target": "file"}),
	}, nil
}

func (t *FileTarget) Open() {
	flags := os.O_CREATE | os.O_WRONLY
	if t.append {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC
	}

	// Ensure directory exists
	dir := filepath.Dir(t.path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.log.Errorf("Failed to create directory: %s", err)
		return
	}

	file, err := os.OpenFile(t.path, flags, t.permissions)
	if err != nil {
		t.log.Errorf("Failed to open file: %s", err)
		return
	}
	t.file = file
}

func (t *FileTarget) Close() {
	if t.file != nil {
		t.file.Close()
	}
}

func (t *FileTarget) Write(messages []*models.Message) (*models.TargetWriteResult, error) {
	t.log.Debugf("Writing %d messages to file...", len(messages))

	safeMessages, oversized := models.FilterOversizedMessages(
		messages,
		t.MaximumAllowedMessageSizeBytes(),
	)

	var sent []*models.Message

	for _, msg := range safeMessages {
		if _, err := t.file.WriteString(string(msg.Data) + "\n"); err != nil {
			return models.NewTargetWriteResult(
				sent,
				[]*models.Message{msg},
				oversized,
				nil,
			), err
		}

		if msg.AckFunc != nil {
			msg.AckFunc()
		}

		sent = append(sent, msg)
	}

	return models.NewTargetWriteResult(
		sent,
		nil,
		oversized,
		nil,
	), nil
}

func (t *FileTarget) GetID() string {
	return "file"
}

func (t *FileTarget) MaximumAllowedMessageSizeBytes() int {
	return 1048576 // 1MB
}

// FileTargetConfigFunction creates a new FileTarget from config
func FileTargetConfigFunction(c *FileTargetConfig) (*FileTarget, error) {
	return newFileTarget(c)
}

// FileTargetAdapter type implements the Pluggable interface
type FileTargetAdapter func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface
func (f FileTargetAdapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface
func (f FileTargetAdapter) ProvideDefault() (interface{}, error) {
	// Provide defaults if any
	cfg := &FileTargetConfig{}
	return cfg, nil
}

// AdaptFileTargetFunc returns a FileTargetAdapter
func AdaptFileTargetFunc(f func(c *FileTargetConfig) (*FileTarget, error)) FileTargetAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*FileTargetConfig)
		if !ok {
			return nil, errors.New("invalid configuration type")
		}
		return f(cfg)
	}
}
