module github.com/snowplow-devops/stream-replicator

go 1.13

require (
	cloud.google.com/go/pubsub v1.9.1
	github.com/aws/aws-lambda-go v1.20.0
	github.com/aws/aws-sdk-go v1.36.8
	github.com/getsentry/sentry-go v0.9.0
	github.com/hashicorp/go-version v1.2.1 // indirect
	github.com/makasim/sentryhook v0.3.0
	github.com/mitchellh/gox v1.0.1 // indirect
	github.com/myesui/uuid v1.0.0 // indirect
	github.com/sirupsen/logrus v1.7.0
	github.com/stretchr/testify v1.6.1
	github.com/twinj/uuid v1.0.0
	github.com/twitchscience/kinsumer v0.0.0-00010101000000-000000000000
	github.com/urfave/cli v1.22.5
	golang.org/x/tools v0.0.0-20201221201019-196535612888 // indirect
	gopkg.in/stretchr/testify.v1 v1.2.2 // indirect
)

replace github.com/twitchscience/kinsumer => github.com/snowplow-devops/kinsumer v0.0.0-20201222120237-1233f129ef85
