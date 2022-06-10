// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package pubsubsource

// Commenting out as it fails on CI - passes on local as I have default creds for a real account
// TODO: Find a way to integration test pubsub

/*
func TestGetSource_WithPubsubSource(t *testing.T) {
	assert := assert.New(t)

	supportedSources := []sourceconfig.SourceConfigPair{PubsubSourceConfigPair}

	defer os.Unsetenv("SOURCE")

	os.Setenv("SOURCE", "pubsub")

	pubsubConfig, err := config.NewConfig()
	assert.NotNil(pubsubConfig)
	assert.Nil(err)

	pubsubSource, err := sourceconfig.GetSource(pubsubConfig, supportedSources)

	assert.NotNil(pubsubSource)
	assert.Nil(err)
	assert.Equal("projects//subscriptions/", pubsubSource.getID())
}
*/
