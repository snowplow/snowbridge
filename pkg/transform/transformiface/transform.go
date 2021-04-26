// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package transformiface

import "github.com/snowplow-devops/stream-replicator/pkg/models"

type TransformationFunction func([]*models.Message) ([]*models.Message, []*models.Message, error)

type TransformationApplyFunction func([]*models.Message) (*models.TransformationResult, error)

type TransformationGenerator func(...TransformationFunction) TransformationApplyFunction

type Transformation interface {
	NewTransformation(tranformFunctions ...TransformationFunction) func(messages []*models.Message) (*models.TransformationResult, error)
} // Should this interface also include some kind of model for the specific transformation implementations, like EnrichedToJson?
