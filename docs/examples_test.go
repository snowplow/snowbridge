package docs

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/snowplow-devops/stream-replicator/config"
	"github.com/stretchr/testify/assert"
)

// To run before templating in full_example to home.md.tmpl
func TestHclFullExample(t *testing.T) {
	assert := assert.New(t)

	hclFilename := filepath.Join("assets", "hcl_full_example.hcl")
	t.Setenv("STREAM_REPLICATOR_CONFIG_FILE", hclFilename)

	c, err := config.NewConfig()
	assert.NotNil(c)
	if err != nil {
		t.Fatalf("function NewConfig failed with error: %q", err.Error())
	}

	assert.Equal(true, c.Data.DisableTelemetry)
	assert.Equal("kinesis", c.Data.Source.Use.Name)
	// ... etc... Check everything in the example config.

	os.Clearenv()
	// TODO: Add the bash stuff, then move on.

	envFilename := filepath.Join("assets", "env_full_example.sh")
	cmd := exec.Command("/bin/sh", envFilename)

	bashErr := cmd.Run()
	if bashErr != nil {
		panic(bashErr)
	}

	assert.Equal(true, c.Data.DisableTelemetry)
	assert.Equal("kinesis", c.Data.Source.Use.Name)
	// ... etc... Check everything in the example config.
}

func TestQuick(t *testing.T) {
	// templateHome()
	TemplateTarget()
}
