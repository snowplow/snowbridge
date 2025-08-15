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

package transform

import (
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/snowplow/snowbridge/v3/pkg/models"
)

// CollectorTstampTransformation returns a transformation function attaching collector timestamp to the input message
// This transformation is not like other configurable transformations - it's enabled/disabled based on top-level metric configuration toggle (`metrics.enable_e2e_latency`)
// It doesn't produce invalid data in case of errors - it logs a warning and proceeds with input data as nothing happened.
func CollectorTstampTransformation() TransformationFunction {
	return func(message *models.Message, interState any) (*models.Message, *models.Message, *models.Message, any) {
		parsedEvent, err := IntermediateAsSpEnrichedParsed(interState, message)
		if err != nil {
			log.Warnf("Error while extracting 'collector_tstamp': %s", err)
			return message, nil, nil, nil
		}

		tstamp, err := parsedEvent.GetValue("collector_tstamp")
		if err != nil {
			log.Warnf("Error while extracting 'collector_tstamp': %s", err)
			return message, nil, nil, parsedEvent
		}

		if collectorTstamp, ok := tstamp.(time.Time); ok {
			message.CollectorTstamp = collectorTstamp
		}

		return message, nil, nil, parsedEvent
	}
}
