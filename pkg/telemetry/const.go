package telemetry

import (
	"time"
)

var (
	interval           = time.Hour
	method             = "POST"
	protocol           = "https"
	url                = "telemetry-g.snowplowanalytics.com"
	port               = "443"
	applicationName    = "stream-replicator"
	applicationVersion = "1.0.0"
)
