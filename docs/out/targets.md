Currently supported target names are:

1. `stdout`: Allows for easy debugging
2. `kinesis`: Replicates to an Amazon Kinesis Stream
3. `pubsub`: Replicates to a GCP PubSub Topic
4. `sqs`: Replicates to an Amazon SQS Queue
5. `kafka`: Replicates to a Kafka Topic
6. `eventhub`: Replicates data to an Azure Event Hub
7. `http`: Replicates data through an HTTP request.

## Configuration via file:

If configuring via `.hcl` file, use a `target {}` block, and choose a source using `use "{target_name}" {}`. Accepted arguments for each target's `use` block can be found in the `hcl` tags in the "Configuration API" section below.

Example:

```hcl
target {
  use "pubsub" {
    # ID of the GCP Project
    project_id = "acme-project"

    # Name of the topic to send data into
    topic_name = "some-acme-topic"
  }
}
```

## Configuration via environment variables

If configuring via environment variable, choose a source by setting `TARGET_NAME={target_name}`. Accepted environment variables for each target's arguments can be found in the `env` tags in the "Configuration API" section below.

Example:

```bash
export TARGET_NAME="pubsub"                 \
TARGET_PUBSUB_PROJECT_ID="acme-project"     \
TARGET_PUBSUB_TOPIC_NAME="some-acme-topic"
```

## Authorisation

Authorisation for AWS And GCP cloud technologies are done by leveraging the default Cloud Environment variables (where appropriate).  Each Cloud provides documentation on how this works:

1. [AWS CLI Configuration](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html)
2. [GCP Service Account](https://cloud.google.com/docs/authentication/getting-started)

Where this is not applicable, authorisation is done according to the target's configuration.

## Configuration API

Available targets and their options are configured as detailed below. `hcl:` tags specify the hcl option name, `env:` tags specify the environment variable name.

<details>
<summary>EventHubs</summary>
<pre><code>type EventHubConfig struct {
	EventHubNamespace       string `hcl:"namespace" env:"TARGET_EVENTHUB_NAMESPACE"`
	EventHubName            string `hcl:"name" env:"TARGET_EVENTHUB_NAME"`
	MaxAutoRetries          int    `hcl:"max_auto_retries,optional" env:"TARGET_EVENTHUB_MAX_AUTO_RETRY"`
	MessageByteLimit        int    `hcl:"message_byte_limit,optional" env:"TARGET_EVENTHUB_MESSAGE_BYTE_LIMIT"`
	ChunkByteLimit          int    `hcl:"chunk_byte_limit,optional" env:"TARGET_EVENTHUB_CHUNK_BYTE_LIMIT"`
	ChunkMessageLimit       int    `hcl:"chunk_message_limit,optional" env:"TARGET_EVENTHUB_CHUNK_MESSAGE_LIMIT"`
	ContextTimeoutInSeconds int    `hcl:"context_timeout_in_seconds,optional" env:"TARGET_EVENTHUB_CONTEXT_TIMEOUT_SECONDS"`
	BatchByteLimit          int    `hcl:"batch_byte_limit,optional" env:"TARGET_EVENTHUB_BATCH_BYTE_LIMIT"`
	SetEHPartitionKey       bool   `hcl:"set_eh_partition_key,optional" env:"TARGET_EVENTHUB_SET_EH_PK"`
}</code></pre>
</details>

<details>
<summary>HTTP</summary>
<pre><code>package target // import "github.com/snowplow-devops/stream-replicator/pkg/target"

type HTTPTargetConfig struct {
	HTTPURL                 string `hcl:"url" env:"TARGET_HTTP_URL"`
	ByteLimit               int    `hcl:"byte_limit,optional" env:"TARGET_HTTP_BYTE_LIMIT"`
	RequestTimeoutInSeconds int    `hcl:"request_timeout_in_seconds,optional" env:"TARGET_HTTP_TIMEOUT_IN_SECONDS"`
	ContentType             string `hcl:"content_type,optional" env:"TARGET_HTTP_CONTENT_TYPE"`
	Headers                 string `hcl:"headers,optional" env:"TARGET_HTTP_HEADERS" `
	BasicAuthUsername       string `hcl:"basic_auth_username,optional" env:"TARGET_HTTP_BASICAUTH_USERNAME"`
	BasicAuthPassword       string `hcl:"basic_auth_password,optional" env:"TARGET_HTTP_BASICAUTH_PASSWORD"`
	CertFile                string `hcl:"cert_file,optional" env:"TARGET_HTTP_TLS_CERT_FILE"`
	KeyFile                 string `hcl:"key_file,optional" env:"TARGET_HTTP_TLS_KEY_FILE"`
	CaFile                  string `hcl:"ca_file,optional" env:"TARGET_HTTP_TLS_CA_FILE"`
	SkipVerifyTLS           bool   `hcl:"skip_verify_tls,optional" env:"TARGET_HTTP_TLS_SKIP_VERIFY_TLS"` // false
}
    HTTPTargetConfig configures the destination for records consumed

</code></pre>
</details>

<details>
<summary>Kafka</summary>
<pre><code>type KafkaConfig struct {
	Brokers        string `hcl:"brokers" env:"TARGET_KAFKA_BROKERS"`
	TopicName      string `hcl:"topic_name" env:"TARGET_KAFKA_TOPIC_NAME"`
	TargetVersion  string `hcl:"target_version,optional" env:"TARGET_KAFKA_TARGET_VERSION"`
	MaxRetries     int    `hcl:"max_retries,optional" env:"TARGET_KAFKA_MAX_RETRIES"`
	ByteLimit      int    `hcl:"byte_limit,optional" env:"TARGET_KAFKA_BYTE_LIMIT"`
	Compress       bool   `hcl:"compress,optional" env:"TARGET_KAFKA_COMPRESS"`
	WaitForAll     bool   `hcl:"wait_for_all,optional" env:"TARGET_KAFKA_WAIT_FOR_ALL"`
	Idempotent     bool   `hcl:"idempotent,optional" env:"TARGET_KAFKA_IDEMPOTENT"`
	EnableSASL     bool   `hcl:"enable_sasl,optional" env:"TARGET_KAFKA_ENABLE_SASL"`
	SASLUsername   string `hcl:"sasl_username,optional" env:"TARGET_KAFKA_SASL_USERNAME" `
	SASLPassword   string `hcl:"sasl_password,optional" env:"TARGET_KAFKA_SASL_PASSWORD"`
	SASLAlgorithm  string `hcl:"sasl_algorithm,optional" env:"TARGET_KAFKA_SASL_ALGORITHM"`
	CertFile       string `hcl:"cert_file,optional" env:"TARGET_KAFKA_TLS_CERT_FILE"`
	KeyFile        string `hcl:"key_file,optional" env:"TARGET_KAFKA_TLS_KEY_FILE"`
	CaFile         string `hcl:"ca_file,optional" env:"TARGET_KAFKA_TLS_CA_FILE"`
	SkipVerifyTLS  bool   `hcl:"skip_verify_tls,optional" env:"TARGET_KAFKA_TLS_SKIP_VERIFY_TLS"`
	ForceSync      bool   `hcl:"force_sync_producer,optional" env:"TARGET_KAFKA_FORCE_SYNC_PRODUCER"`
	FlushFrequency int    `hcl:"flush_frequency,optional" env:"TARGET_KAFKA_FLUSH_FREQUENCY"`
	FlushMessages  int    `hcl:"flush_messages,optional" env:"TARGET_KAFKA_FLUSH_MESSAGES"`
	FlushBytes     int    `hcl:"flush_bytes,optional" env:"TARGET_KAFKA_FLUSH_BYTES"`
}</code></pre>
</details>

<details>
<summary>Kinesis</summary>
<pre><code>type KinesisTargetConfig struct {
	StreamName string `hcl:"stream_name" env:"TARGET_KINESIS_STREAM_NAME"`
	Region     string `hcl:"region" env:"TARGET_KINESIS_REGION"`
	RoleARN    string `hcl:"role_arn,optional" env:"TARGET_KINESIS_ROLE_ARN"`
}</code></pre>
</details>

<details>
<summary>PubSub</summary>
<pre><code>type PubSubTargetConfig struct {
	ProjectID string `hcl:"project_id" env:"TARGET_PUBSUB_PROJECT_ID"`
	TopicName string `hcl:"topic_name" env:"TARGET_PUBSUB_TOPIC_NAME"`
}</code></pre>
</details>

<details>
<summary>SQS</summary>
<pre><code>type SQSTargetConfig struct {
	QueueName string `hcl:"queue_name" env:"TARGET_SQS_QUEUE_NAME"`
	Region    string `hcl:"region" env:"TARGET_SQS_REGION"`
	RoleARN   string `hcl:"role_arn,optional" env:"TARGET_SQS_ROLE_ARN"`
}</code></pre>
</details>

<details>
<summary>stdout</summary>
<pre><code>// stdout requires no configuration beyond `use "stdout"`, or `TARGET_NAME="stdout"`.</code></pre>
</details>
