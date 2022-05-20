package telemetry

import (
	"time"

	"github.com/snowplow-devops/stream-replicator/cmd"
)

var (
	interval           = time.Hour
	method             = "POST"
	protocol           = "https"
	url                = "telemetry-g.snowplowanalytics.com"
	port               = "443"
	applicationName    = "stream-replicator"
	applicationVersion = cmd.AppVersion
)
