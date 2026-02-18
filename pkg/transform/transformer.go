package transform

import (
	"runtime"
	"sync"

	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/observer"
)

type Transformer struct {
	transformFunction TransformationApplyFunction
	input             <-chan *models.Message
	output            chan<- *models.TransformationResult
	observer          *observer.Observer
	workerPool        int
}

func NewTransformer(
	transformFunction TransformationApplyFunction,
	input <-chan *models.Message,
	output chan<- *models.TransformationResult,
	observer *observer.Observer,
	workerPool int) *Transformer {
	return &Transformer{
		transformFunction: transformFunction,
		input:             input,
		output:            output,
		observer:          observer,
		workerPool:        workerPool,
	}
}

func (t Transformer) Start() {
	// Close output channel when all workers are done
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
		// Spawn goroutine per worker, not per task/message.
		wg.Go(func() {
			// Consume from input. This is populated by source independently!
			// Input channel is a way for transformer worker to backpressure/throttle source.
			for msg := range t.input {
				// Do some work...
				transformed := t.transformFunction(msg)
				t.observer.Transformed(transformed)

				// Send to output channel. This output channel is then later consumed by targets
				// Output channel is a way for targets to backpressure/throttle transformer workers
				t.output <- transformed
			}
		})
	}

	// Wait for all workers to finish processing
	// Each worker finishes when input channel is closed by a source
	wg.Wait()
}
