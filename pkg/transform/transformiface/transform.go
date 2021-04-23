package transformiface

import "github.com/snowplow-devops/stream-replicator/pkg/models"

type Transformation interface {
	ApplyTransformations(messages []*models.Message, tranformFunctions ...func(*models.Message) (*models.Message, error)) (*models.TransformationResult, error)
	TransformToJson(*models.Message) (*models.Message, error)
	// Filter(messages []*models.Message) (to add in future)
}
