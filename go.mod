module github.com/snowplow-devops/stream-replicator

go 1.16

require (
	cloud.google.com/go v0.91.1 // indirect
	cloud.google.com/go/pubsub v1.14.0
	github.com/Azure/azure-amqp-common-go/v3 v3.1.0 // indirect
	github.com/Azure/azure-event-hubs-go/v3 v3.3.12
	github.com/Azure/azure-sdk-for-go v56.2.0+incompatible // indirect
	github.com/Azure/go-amqp v0.13.11 // indirect
	github.com/Azure/go-autorest/autorest v0.11.19 // indirect
	github.com/Azure/go-autorest/autorest/adal v0.9.14 // indirect
	github.com/Shopify/sarama v1.29.1
	github.com/aws/aws-lambda-go v1.26.0
	github.com/aws/aws-sdk-go v1.40.22
	github.com/caarlos0/env/v6 v6.6.2
	github.com/cpuguy83/go-md2man/v2 v2.0.1 // indirect
	github.com/form3tech-oss/jwt-go v3.2.5+incompatible // indirect
	github.com/getsentry/sentry-go v0.11.0
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1
	github.com/hashicorp/go-version v1.3.0 // indirect
	github.com/jpillora/backoff v1.0.0 // indirect
	github.com/klauspost/compress v1.13.4 // indirect
	github.com/mitchellh/gox v1.0.1 // indirect
	github.com/mitchellh/mapstructure v1.4.1 // indirect
	github.com/myesui/uuid v1.0.0 // indirect
	github.com/pierrec/lz4 v2.6.1+incompatible // indirect
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.8.1
	github.com/smira/go-statsd v1.3.2
	github.com/snowplow-devops/go-retry v0.0.0-20210106090855-8989bbdbae1c
	github.com/snowplow-devops/go-sentryhook v0.0.0-20210106082031-21bf7f9dac2a
	github.com/snowplow/snowplow-golang-analytics-sdk v0.1.0
	github.com/stretchr/testify v1.7.0
	github.com/twinj/uuid v1.0.0
	github.com/twitchscience/kinsumer v0.0.0-20210611163023-da24975e2c91
	github.com/urfave/cli v1.22.5
<<<<<<< HEAD
	github.com/xdg/scram v0.0.0-20180814205039-7eeb5667e42c
	golang.org/x/lint v0.0.0-20210508222113-6edffad5e616 // indirect
	golang.org/x/sys v0.0.0-20210630005230-0f9fa26af87c // indirect
	golang.org/x/tools v0.1.4 // indirect
=======
	github.com/xdg/scram v1.0.3
	golang.org/x/crypto v0.0.0-20210812204632-0ba0e8f03122 // indirect
	golang.org/x/mod v0.5.0 // indirect
	golang.org/x/net v0.0.0-20210813160813-60bc85c4be6d // indirect
	golang.org/x/oauth2 v0.0.0-20210810183815-faf39c7919d5 // indirect
	golang.org/x/sys v0.0.0-20210816074244-15123e1e1f71 // indirect
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/api v0.54.0 // indirect
	google.golang.org/genproto v0.0.0-20210813162853-db860fec028c // indirect
	google.golang.org/grpc v1.40.0 // indirect
>>>>>>> c3ab685 (Tweak eventhubs client setup)
	gopkg.in/stretchr/testify.v1 v1.2.2 // indirect
)

replace github.com/twitchscience/kinsumer => github.com/snowplow-devops/kinsumer v0.0.0-20201222120237-1233f129ef85
