package engine

import (
	"testing"
)

func TestOpaFunc(t *testing.T) {

	opa, err := NewOPAEngine(&OPAEngineConfig{})
	if err != nil {
		panic(err)
	}

	opaFunc := opa.MakeFunction()

	opaFunc(messages[0], nil)

}
