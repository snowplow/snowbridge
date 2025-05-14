package common

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

type KinesisV2API interface {
	CreateStream(context.Context, *kinesis.CreateStreamInput, ...func(*kinesis.Options)) (*kinesis.CreateStreamOutput, error)
	DeleteStream(context.Context, *kinesis.DeleteStreamInput, ...func(*kinesis.Options)) (*kinesis.DeleteStreamOutput, error)
	DescribeStream(context.Context, *kinesis.DescribeStreamInput, ...func(*kinesis.Options)) (*kinesis.DescribeStreamOutput, error)
	PutRecords(context.Context, *kinesis.PutRecordsInput, ...func(*kinesis.Options)) (*kinesis.PutRecordsOutput, error)
}
