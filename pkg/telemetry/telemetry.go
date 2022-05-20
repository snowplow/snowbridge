package telemetry

import (
	"fmt"
	"net/http"
	"time"

	log "github.com/sirupsen/logrus"
	conf "github.com/snowplow-devops/stream-replicator/config"
	gt "github.com/snowplow/snowplow-golang-tracker/v2/tracker"
	"github.com/twinj/uuid"
)

// config holds the configuration for telemetry
type config struct {
	enable             bool
	interval           time.Duration
	method             string
	url                string
	protocol           string
	port               string
	userProvidedID     string
	applicationName    string
	applicationVersion string
	appGeneratedID     string
}

func newTelemetryWithConfig(cfg *conf.Config) *config {
	return &config{
		enable:             cfg.Data.EnableTelemetry,
		interval:           interval,
		method:             method,
		protocol:           protocol,
		url:                url,
		port:               port,
		userProvidedID:     cfg.Data.UserProvidedID,
		applicationName:    applicationName,
		applicationVersion: applicationVersion,
		appGeneratedID:     uuid.NewV4().String(),
	}
}

func initTelemetry(telemetry *config) {
	storage := gt.InitStorageMemory()
	emitter := gt.InitEmitter(
		gt.RequireCollectorUri(fmt.Sprintf(`%s:%s`, telemetry.url, telemetry.port)),
		gt.OptionRequestType(telemetry.method),
		gt.OptionProtocol(telemetry.protocol),
		gt.OptionCallback(func(goodResults []gt.CallbackResult, badResults []gt.CallbackResult) {
			for _, goodResult := range goodResults {
				if goodResult.Status != http.StatusOK {
					log.WithFields(log.Fields{
						"error_code": goodResult.Status,
					}).Debugf("Error sending telemetry event")
					return
				}
			}
			for _, badResult := range badResults {
				if badResult.Status != http.StatusOK {
					log.WithFields(log.Fields{
						"error_code": badResult.Status,
					}).Debugf("Error sending telemetry event")
					return
				}
			}
			log.Info(`Telemetry event sent successfully`)
		}),
		gt.OptionStorage(storage),
	)

	tracker := gt.InitTracker(
		gt.RequireEmitter(emitter),
		gt.OptionNamespace("telemetry"),
		gt.OptionAppId(telemetry.applicationName),
	)

	ticker := time.NewTicker(telemetry.interval)

	go func() {
		makeAndTrackHeartbeat(telemetry, tracker)
		for {
			<-ticker.C
			makeAndTrackHeartbeat(telemetry, tracker)
		}
	}()
}

func makeAndTrackHeartbeat(telemetry *config, tracker *gt.Tracker) {
	event := makeHeartbeatEvent(*telemetry)

	tracker.TrackSelfDescribingEvent(gt.SelfDescribingEvent{
		Event:         event,
		Timestamp:     nil,
		EventId:       nil,
		TrueTimestamp: nil,
		Contexts:      nil,
		Subject:       nil,
	})
}

// InitTelemetryWithCollector initialises telemetry
func InitTelemetryWithCollector(cfg *conf.Config) {
	telemetry := newTelemetryWithConfig(cfg)
	initTelemetry(telemetry)
}

func makeHeartbeatEvent(service config) *gt.SelfDescribingJson {
	payload := gt.InitPayload()

	payload.Add(`userProvidedId`, &service.userProvidedID)
	payload.Add(`applicationName`, &service.applicationName)
	payload.Add(`applicationVersion`, &service.applicationVersion)
	payload.Add(`appGeneratedId`, &service.appGeneratedID)

	selfDescJSON := gt.InitSelfDescribingJson(
		`iglu:com.snowplowanalytics.oss/oss_context/jsonschema/1-0-1`, payload.Get())
	return selfDescJSON
}
