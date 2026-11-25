package monitoring

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/snowplow/snowbridge/v6/pkg/models"
)

// AggregatedError holds aggregated error information
// for reporting by metadata reporter
type AggregatedError struct {
	Code        string          `json:"code"`
	Description string          `json:"description"`
	Count       int             `json:"count"`
	Sample      json.RawMessage `json:"sample,omitempty"`
}

// MetadataSender describes the interface for how to send metadata events
type MetadataSender interface {
	Do(req *http.Request) (*http.Response, error)
}

type MetadataReporterer interface {
	Send(b *models.ObserverBuffer, periodStart, periodEnd time.Time)
}

// errorSamplesPerPeriod caps the samples a reporter with include_error_samples sends per period,
// which bounds both the memory held for them and the size of the metadata event
const errorSamplesPerPeriod = 10

type errorSampler interface {
	maxErrorSamples() int
}

// MaxErrorSamples returns how many invalid error samples the observer should keep per period for
// this reporter, 0 when it sends none
func MaxErrorSamples(r MetadataReporterer) int {
	if s, ok := r.(errorSampler); ok {
		return s.maxErrorSamples()
	}
	return 0
}

type MetadataReporter struct {
	client   MetadataSender
	endpoint string
	log      *logrus.Entry

	appName        string
	appVersion     string
	tags           map[string]string
	includeSamples bool
}

func NewMetadataReporter(appName, appVersion string, client MetadataSender, endpoint string, tags map[string]string, includeSamples bool) *MetadataReporter {
	return &MetadataReporter{
		appName:        appName,
		appVersion:     appVersion,
		client:         client,
		endpoint:       endpoint,
		tags:           tags,
		includeSamples: includeSamples,
		log:            logrus.WithFields(logrus.Fields{"name": "MetadataReporter"}),
	}
}

type MetadataEvent struct {
	Schema string          `json:"schema"`
	Data   MetadataWrapper `json:"data"`
}

type MetadataWrapper struct {
	AppName       string            `json:"appName"`
	AppVersion    string            `json:"appVersion"`
	PeriodStart   string            `json:"periodStart"`
	PeriodEnd     string            `json:"periodEnd"`
	Success       int64             `json:"successCount"`
	Filtered      int64             `json:"filteredCount"`
	Failed        int64             `json:"failedCount"`
	Invalid       int64             `json:"invalidCount"`
	InvalidErrors []AggregatedError `json:"invalidErrors,omitempty"`
	FailedErrors  []AggregatedError `json:"failedErrors,omitempty"` // transient/retryable
	Tags          map[string]string `json:"tags"`
}

func (mr *MetadataReporter) Send(b *models.ObserverBuffer, periodStart, periodEnd time.Time) {
	aggrInvalid := invalidErrorsToSlice(b.InvalidErrors, mr.includeSamples)
	aggrFailed := failedErrorsToSlice(b.FailedErrors)

	event := MetadataEvent{
		Schema: "iglu:com.snowplowanalytics.snowplow/event_forwarding_metrics/jsonschema/1-0-0",
		Data: MetadataWrapper{
			AppName:       mr.appName,
			AppVersion:    mr.appVersion,
			PeriodStart:   periodStart.Format(time.RFC3339),
			PeriodEnd:     periodEnd.Format(time.RFC3339),
			Success:       b.MsgSent,
			Filtered:      b.MsgFiltered,
			Failed:        b.MsgFailed,
			Invalid:       b.InvalidMsgSent,
			InvalidErrors: aggrInvalid,
			FailedErrors:  aggrFailed,
			Tags:          mr.tags,
		},
	}

	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(event); err != nil {
		mr.log.Warnf("failed to marshall event: %s", err)
		return
	}

	var lastErr error
	for attempt := 0; attempt <= 5; attempt++ {
		if attempt > 0 {
			time.Sleep(100 * time.Millisecond)
		}

		if err := mr.sendRequest(body.Bytes()); err != nil {
			lastErr = err
			mr.log.Infof("attempt %d to send metadata event failed: %s", attempt+1, err)
			continue
		}
		return
	}

	if mr.includeSamples {
		// The body is not logged as its samples hold raw event data
		mr.log.Warnf("failed to send metadata event for period %s - %s (success: %d, filtered: %d, failed: %d, invalid: %d): %s",
			event.Data.PeriodStart, event.Data.PeriodEnd, event.Data.Success, event.Data.Filtered, event.Data.Failed, event.Data.Invalid, lastErr)
		return
	}
	mr.log.Warnf("failed to send metadata event %s: %s", bytes.TrimRight(body.Bytes(), "\n"), lastErr)
}

func (mr *MetadataReporter) maxErrorSamples() int {
	if mr.includeSamples {
		return errorSamplesPerPeriod
	}
	return 0
}

func (mr *MetadataReporter) sendRequest(body []byte) error {
	req, err := http.NewRequest(http.MethodPost, mr.endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := mr.client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		if _, err := io.Copy(io.Discard, resp.Body); err != nil {
			mr.log.Warn(err.Error())
		}
		if err := resp.Body.Close(); err != nil {
			mr.log.Warn(err.Error())
		}
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("server responded with status %d", resp.StatusCode)
	}
	return nil
}

func invalidErrorsToSlice(errsMap map[models.MetadataCodeDescription]*models.ErrorAggregate, includeSamples bool) []AggregatedError {
	var aggrErrors []AggregatedError

	for err, aggregate := range errsMap {
		aggrError := AggregatedError{
			Code:        err.Code,
			Description: err.Description,
			Count:       aggregate.Count,
		}
		if includeSamples {
			aggrError.Sample = aggregate.Sample
		}
		aggrErrors = append(aggrErrors, aggrError)
	}
	return aggrErrors
}

func failedErrorsToSlice(errsMap map[models.MetadataCodeDescription]int) []AggregatedError {
	var aggrErrors []AggregatedError

	for err, v := range errsMap {
		aggrErrors = append(aggrErrors, AggregatedError{
			Code:        err.Code,
			Description: err.Description,
			Count:       v,
		})
	}
	return aggrErrors
}

type fanOut []MetadataReporterer

// FanOut sends every period to all reporters concurrently
func FanOut(reporters []MetadataReporterer) MetadataReporterer {
	return fanOut(reporters)
}

func (f fanOut) maxErrorSamples() int {
	samples := 0
	for _, r := range f {
		samples = max(samples, MaxErrorSamples(r))
	}
	return samples
}

func (f fanOut) Send(b *models.ObserverBuffer, periodStart, periodEnd time.Time) {
	var wg sync.WaitGroup
	for _, r := range f {
		wg.Go(func() { r.Send(b, periodStart, periodEnd) })
	}
	wg.Wait()
}
