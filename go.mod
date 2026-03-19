module github.com/snowplow/snowbridge/v3

go 1.25.8

require (
	cloud.google.com/go v0.123.0 // indirect
	cloud.google.com/go/pubsub v1.50.1
	github.com/Azure/azure-event-hubs-go/v3 v3.6.2
	github.com/Azure/azure-sdk-for-go v68.0.0+incompatible // indirect
	github.com/Azure/go-amqp v1.5.1
	github.com/Azure/go-autorest/autorest v0.11.30 // indirect
	github.com/Azure/go-autorest/autorest/adal v0.9.24 // indirect
	github.com/IBM/sarama v1.47.0
	github.com/cpuguy83/go-md2man/v2 v2.0.7 // indirect
	github.com/getsentry/sentry-go v0.43.0
	github.com/golang-jwt/jwt/v4 v4.5.2 // indirect
	github.com/golang/groupcache v0.0.0-20241129210726-2c02b8208cf8 // indirect
	github.com/google/uuid v1.6.0
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1
	github.com/jpillora/backoff v1.0.0 // indirect
	github.com/klauspost/compress v1.18.4 // indirect
	github.com/mitchellh/mapstructure v1.5.0
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.9.4
	github.com/smira/go-statsd v1.3.4
	github.com/snowplow-devops/go-sentryhook v0.0.0-20210106082031-21bf7f9dac2a
	github.com/snowplow/snowplow-golang-analytics-sdk v0.4.0
	github.com/stretchr/testify v1.11.1
	github.com/twitchscience/kinsumer v0.0.0-20240315191529-9a48088063ec
	github.com/urfave/cli v1.22.17
	github.com/xdg/scram v1.0.5
	golang.org/x/crypto v0.49.0 // indirect
	golang.org/x/net v0.52.0 // indirect
	golang.org/x/oauth2 v0.36.0
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/text v0.35.0 // indirect
	google.golang.org/api v0.272.0
	google.golang.org/genproto v0.0.0-20260316180232-0b37fe3546d5 // indirect
	google.golang.org/grpc v1.79.3
)

require (
	github.com/avast/retry-go/v4 v4.7.0
	github.com/aws/aws-sdk-go-v2 v1.41.4
	github.com/aws/aws-sdk-go-v2/config v1.32.12
	github.com/aws/aws-sdk-go-v2/credentials v1.19.12
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.56.2
	github.com/aws/aws-sdk-go-v2/service/kinesis v1.43.3
	github.com/aws/aws-sdk-go-v2/service/sqs v1.42.24
	github.com/aws/aws-sdk-go-v2/service/sts v1.41.9
	github.com/davecgh/go-spew v1.1.1
	github.com/dop251/goja v0.0.0-20260311135729-065cd970411c
	github.com/google/go-cmp v0.7.0
	github.com/hashicorp/hcl/v2 v2.24.0
	github.com/itchyny/gojq v0.12.18
	github.com/josephburnett/jd/v2 v2.5.0
	github.com/json-iterator/go v1.1.12
	github.com/snowplow/snowplow-golang-tracker/v2 v2.4.1
	github.com/twinj/uuid v1.0.0
	github.com/zclconf/go-cty v1.18.0
)

require (
	cloud.google.com/go/auth v0.18.2 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.8 // indirect
	cloud.google.com/go/compute/metadata v0.9.0 // indirect
	cloud.google.com/go/iam v1.5.3 // indirect
	cloud.google.com/go/pubsub/v2 v2.4.0 // indirect
	github.com/Azure/azure-amqp-common-go/v4 v4.2.0 // indirect
	github.com/Azure/go-autorest v14.2.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest/date v0.3.1 // indirect
	github.com/Azure/go-autorest/autorest/to v0.4.1 // indirect
	github.com/Azure/go-autorest/autorest/validation v0.3.2 // indirect
	github.com/Azure/go-autorest/logger v0.2.2 // indirect
	github.com/Azure/go-autorest/tracing v0.6.1 // indirect
	github.com/agext/levenshtein v1.2.3 // indirect
	github.com/apparentlymart/go-textseg/v15 v15.0.0 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.7 // indirect
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.20.35 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.20 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.20 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.20 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.32.13 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.11.20 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.20 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.0.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.30.13 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.35.17 // indirect
	github.com/aws/smithy-go v1.24.2 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/devigned/tab v0.1.1 // indirect
	github.com/dlclark/regexp2 v1.11.5 // indirect
	github.com/eapache/go-resiliency v1.7.0 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-sourcemap/sourcemap v2.1.4+incompatible // indirect
	github.com/google/pprof v0.0.0-20260302011040-a15ffb7f9dcc // indirect
	github.com/google/s2a-go v0.1.9 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.14 // indirect
	github.com/googleapis/gax-go/v2 v2.19.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-memdb v1.3.5 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/itchyny/timefmt-go v0.1.7 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.4 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/mattn/go-sqlite3 v2.0.2+incompatible // indirect
	github.com/mitchellh/go-wordwrap v1.0.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/myesui/uuid v1.0.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20250401214520-65e299d6c5c9 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/xdg/stringprep v1.0.3 // indirect
	go.einride.tech/aip v0.83.1 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.67.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.67.0 // indirect
	go.opentelemetry.io/otel v1.42.0 // indirect
	go.opentelemetry.io/otel/metric v1.42.0 // indirect
	go.opentelemetry.io/otel/sdk v1.42.0 // indirect
	go.opentelemetry.io/otel/trace v1.42.0 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/mod v0.34.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/time v0.15.0 // indirect
	golang.org/x/tools v0.43.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260316180232-0b37fe3546d5 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260316180232-0b37fe3546d5 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
	gopkg.in/stretchr/testify.v1 v1.2.2 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/twitchscience/kinsumer => github.com/snowplow-devops/kinsumer v1.7.0
