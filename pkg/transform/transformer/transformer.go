package transform

import (
	"encoding/base64"
	"runtime"
	"sync"

	"github.com/snowplow/snowbridge/v5/pkg/decompression"
	"github.com/snowplow/snowbridge/v5/pkg/fanout"
	"github.com/snowplow/snowbridge/v5/pkg/models"
	"github.com/snowplow/snowbridge/v5/pkg/observer"

	"github.com/snowplow/snowbridge/v5/pkg/transform"
)

type Transformer struct {
	transformFunction transform.TransformationApplyFunction
	decompressionCfg  decompression.Config
	input             <-chan *models.Message
	output            chan<- *models.TransformationResult
	observer          *observer.Observer
	workerPool        int
}

func NewTransformer(
	transformFunction transform.TransformationApplyFunction,
	decompressionCfg decompression.Config,
	input <-chan *models.Message,
	output chan<- *models.TransformationResult,
	observer *observer.Observer,
	workerPool int) *Transformer {
	return &Transformer{
		transformFunction: transformFunction,
		decompressionCfg:  decompressionCfg,
		input:             input,
		output:            output,
		observer:          observer,
		workerPool:        workerPool,
	}
}

func (t *Transformer) Start() {
	defer close(t.output)

	var numberOfWorkers int
	if t.workerPool > 0 {
		numberOfWorkers = t.workerPool
	} else {
		// Gomaxprocs returns the value the scheduler is using for parallelism.
		// Both gomaxprocs and runtime.NumCPU() round down to the nearest integer, with a minimum of 1.
		// So this isn't ideal - if the CPU available isn't a round number we can't configure things accurately.
		// We add one here to ensure we don't leave CPU wasted, at the cost of scheduler overhead.
		numberOfWorkers = runtime.GOMAXPROCS(0) + 1
	}

	var wg sync.WaitGroup
	for range numberOfWorkers {
		wg.Go(func() {
			for parent := range t.input {
				t.processMessage(parent)
			}
		})
	}

	wg.Wait()
}

// processMessage decompresses parent into N child records, applies the
// transform function to each, and emits results. Bad records from
// decompression are emitted as Invalid TransformationResults — the router
// already handles routing those to the failure target.
func (t *Transformer) processMessage(parent *models.Message) {
	fanout.Run(parent, func(createChild fanout.CreateChild) {

		// The range loop blocks until the input is fully processed. Channel
		// sends inside the loop body naturally backpressure the reader.
		for res := range decompression.Decompress(t.decompressionCfg, parent.Data) {
			switch res.Kind {
			case decompression.ResultRecord:
				child := createChild(res.Record.Data)
				t.output <- t.transformFunction(child)

			case decompression.ResultBad:
				encoded := []byte(base64.StdEncoding.EncodeToString(parent.Data))
				msg := createChild(encoded)
				msg.SetError(&models.DecompressionError{
					ErrorCode:   res.Bad.Kind.String(),
					SafeMessage: res.Bad.Message,
					Err:         res.Bad.Err,
				})
				t.output <- models.NewTransformationResult(nil, nil, msg)
			}
		}
	})
}
