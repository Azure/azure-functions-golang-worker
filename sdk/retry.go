package sdk

type RetryContext struct {
	RetryCount    int
	MaxRetryCount int
	RpcException  error
}

type RetryPolicy string

const (
	MaxRetryCount   RetryPolicy = "max_retry_count"
	Strategy        RetryPolicy = "strategy"
	DelayInterval   RetryPolicy = "delay_interval"
	MinimumInterval RetryPolicy = "minimum_interval"
	MaximumInterval RetryPolicy = "maximum_interval"
)
