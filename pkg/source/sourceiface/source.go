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

package sourceiface

import "github.com/snowplow/snowbridge/v3/pkg/observer"

// Source describes the interface for how to read the data pulled from the source
type Source interface {
	Read(sf *SourceFunctions) error
	Stop()
	GetID() string
	SetObserver(*observer.Observer)
}

// NoOpObserver provides a default no-op implementation of SetObserver
// for sources that don't need observer functionality
type NoOpObserver struct{}

// SetObserver does nothing - no-op implementation
func (NoOpObserver) SetObserver(*observer.Observer) {}
