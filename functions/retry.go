package functions

import (
	"fmt"
	"time"

	pb "github.com/azure/azure-functions-golang-worker/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

type RetryOptions struct {
	MaxRetryCount   int
	DelayInterval   *time.Duration
	MinimumInterval *time.Duration
	MaximumInterval *time.Duration
	Strategy        RetryStrategy
}

type RetryStrategy int

const (
	ExponentialBackoff RetryStrategy = iota
	FixedDelay
)

func BuildRpcRetry(r *RetryOptions) *pb.RpcRetryOptions {
	if r == nil {
		return nil
	}
	retryOptions := &pb.RpcRetryOptions{}
	if r.Strategy == ExponentialBackoff {
		retryOptions.RetryStrategy = *pb.RpcRetryOptions_exponential_backoff.Enum()
		retryOptions.MaximumInterval = durationpb.New(*r.MaximumInterval)
		retryOptions.MinimumInterval = durationpb.New(*r.MinimumInterval)
	} else if r.Strategy == FixedDelay {
		retryOptions.RetryStrategy = *pb.RpcRetryOptions_fixed_delay.Enum()
		retryOptions.DelayInterval = durationpb.New(*r.DelayInterval)
	} else {
		panic(fmt.Sprintf("Unknown retry strategy: %v", r.Strategy))
	}
	return retryOptions
}
