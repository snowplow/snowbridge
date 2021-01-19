// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package sourceiface

// Source describes the interface for how to read the data pulled from the source
type Source interface {
	Read(sf *SourceFunctions) error
	Stop()
	GetID() string
}
