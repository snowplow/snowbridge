/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package config

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/v3/assets"
	"github.com/snowplow/snowbridge/v3/pkg/statsreceiver"
)

func TestCreateObserverComponentHCL(t *testing.T) {
	testCases := []struct {
		File     string
		Plug     Pluggable
		Expected any
	}{
		{
			File: "observer.hcl",
			Plug: testStatsDAdapter(testStatsDFunc),
			Expected: &statsreceiver.StatsDStatsReceiverConfig{
				Address: "test.localhost",
				Prefix:  "snowplow.test",
				Tags:    "{\"testKey\": \"testValue\"}",
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.File, func(t *testing.T) {
			assert := assert.New(t)

			filename := filepath.Join(assets.AssetsRootDir, "test", "config", "configs", tt.File)
			t.Setenv("SNOWBRIDGE_CONFIG_FILE", filename)

			c, err := NewConfig()
			assert.NotNil(c)
			if err != nil {
				t.Fatalf("function NewConfig failed with error: %q", err.Error())
			}

			assert.Equal(2, c.Data.StatsReceiver.TimeoutSec)
			assert.Equal(20, c.Data.StatsReceiver.BufferSec)

			use := c.Data.StatsReceiver.Receiver
			decoderOpts := &DecoderOptions{
				Input: use.Body,
			}

			result, err := c.CreateComponent(tt.Plug, decoderOpts)
			assert.NotNil(result)
			assert.Nil(err)

			if !reflect.DeepEqual(result, tt.Expected) {
				t.Errorf("GOT:\n%s\nEXPECTED:\n%s",
					spew.Sdump(result),
					spew.Sdump(tt.Expected))
			}
		})
	}
}

// StatsD
func testStatsDAdapter(f func(c *statsreceiver.StatsDStatsReceiverConfig) (*statsreceiver.StatsDStatsReceiverConfig, error)) statsreceiver.StatsDStatsReceiverAdapter {
	return func(i any) (any, error) {
		cfg, ok := i.(*statsreceiver.StatsDStatsReceiverConfig)
		if !ok {
			return nil, errors.New("invalid input, expected StatsDStatsReceiverConfig")
		}

		return f(cfg)
	}

}

func testStatsDFunc(c *statsreceiver.StatsDStatsReceiverConfig) (*statsreceiver.StatsDStatsReceiverConfig, error) {

	return c, nil
}
