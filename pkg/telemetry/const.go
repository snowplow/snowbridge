package telemetry

import (
	"time"

	"github.com/snowplow/snowbridge/cmd"
)

var (
	interval           = time.Hour
	method             = "POST"
	protocol           = "https"
	url                = "telemetry-g.snowplowanalytics.com"
	port               = "443"
	applicationName    = "snowbridge"
	applicationVersion = cmd.AppVersion
)
