# Batching model

Messages are processed in batches according to how the source provides data. The Kinesis and Pubsub sources provide data in message-by-message, data is handled in batches of 1 message. The SQS source is batched according to how the SQS queue returns messages.

Transformations always handle individual messages at a time.

If the source provides the data in batch, the Kinesis, SQS, EventHub and Kafka targets can chunk the data into smaller batches before sending the requests. The EventHub target can further batch the data according to partitionKey, if set - which is a feature of the EventHub client specifically. The Pubsub and HTTP targets handle messages individually at present.

