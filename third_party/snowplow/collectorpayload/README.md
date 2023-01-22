# collectorpayload

This module is forked from the [Scala equivalent](https://github.com/snowplow/snowplow/tree/master/2-collectors/thrift-schemas/collector-payload-1).

To generate the `gen-go/model1` directory:

1. Install `thrift`: https://thrift.apache.org/download
2. From this directory run `thrift -r --gen go collector_payload_1.thrift`

_NOTE_: Running `make format` will fix formatting issues in the auto-generated code.

To see how to use the library have a look at `collector_payload_test.go` which contains helper functions for seralizing and deserializing payloads.
