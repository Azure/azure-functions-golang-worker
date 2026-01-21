package sdk

import "time"

// RetryOptions defines the retry policy for a function.
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
