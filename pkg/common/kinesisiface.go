package common

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

// KinesisV2API describes methods which must be implemented by a client to communicate with Kinesis
type KinesisV2API interface {
	CreateStream(context.Context, *kinesis.CreateStreamInput, ...func(*kinesis.Options)) (*kinesis.CreateStreamOutput, error)
	DeleteStream(context.Context, *kinesis.DeleteStreamInput, ...func(*kinesis.Options)) (*kinesis.DeleteStreamOutput, error)
	DescribeStream(context.Context, *kinesis.DescribeStreamInput, ...func(*kinesis.Options)) (*kinesis.DescribeStreamOutput, error)
	PutRecords(context.Context, *kinesis.PutRecordsInput, ...func(*kinesis.Options)) (*kinesis.PutRecordsOutput, error)
	ListShards(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error)
	GetRecords(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error)
	PutRecord(ctx context.Context, params *kinesis.PutRecordInput, optFns ...func(*kinesis.Options)) (*kinesis.PutRecordOutput, error)
	MergeShards(ctx context.Context, params *kinesis.MergeShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.MergeShardsOutput, error)
	GetShardIterator(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error)
}
