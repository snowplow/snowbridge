package engine

import (
	"fmt"
	"testing"

	sdktest "github.com/open-policy-agent/opa/sdk/test"
	"github.com/stretchr/testify/assert"
)

func TestOpaFunc(t *testing.T) {
	server, err := sdktest.NewServer(sdktest.MockBundle("/bundles/bundle.tar.gz", map[string]string{
		"example.rego": `
				package snp

				default drop := false

				drop {
					input.app_id == "test-data3"
          input.contexts_nl_basjes_yauaa_context_1[_].test1.test2[_].test3 == "testValue"
				}

        drop {
					input.app_id == "test-data1"
        }

			`,
	}))
	if err != nil {
		panic(err)
	}

	defer server.Stop()

	config := fmt.Sprintf(`{
		"services": {
			"test": {
        "url": %q
			}
		},
		"bundles": {
			"test": {
				"resource": "/bundles/bundle.tar.gz"
			}
		},
		"decision_logs": {
			"console": false
		}
	}`, server.URL())

  opa, err := NewOPAEngine(&OPAEngineConfig{OPAConfig: config})
	if err != nil {
		panic(err)
	}

	opaFunc := opa.MakeFunction()

  success, filtered, failure, _ := opaFunc(messages[2], nil)
  assert.Nil(t, success)
  assert.NotNil(t, filtered)
  assert.Nil(t, failure)

  success, filtered, failure, _ = opaFunc(messages[0], nil)
  assert.Nil(t, success)
  assert.NotNil(t, filtered)
  assert.Nil(t, failure)

  success, filtered, failure, _ = opaFunc(messages[1], nil)
  assert.NotNil(t, success)
  assert.Nil(t, filtered)
  assert.Nil(t, failure)

}
