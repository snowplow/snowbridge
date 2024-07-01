package generic

import (
	"testing"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclparse"
	"github.com/stretchr/testify/assert"
)

var supportedTargets = map[string]providerOf[target]{
	"testTarget": &testProvider{},
}

func Test_CreatingTarget(t *testing.T) {
	assert := assert.New(t)
	hclFile, _ := hclparse.NewParser().ParseHCL([]byte(`test_string = "ateststring"`), "placeholder.hcl")
	input := hclFile.Body

	target, _ := supportedTargets["testTarget"].Provide(input, &hcl.EvalContext{})

	output := target.Write("Test string")

	assert.Equal("Written: Test string. This is value in config - ateststring", output)

}
