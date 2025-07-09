package functions

type Context struct {
	InvocationId   string
	FunctionId     string
	TraceContext   *TraceContext
	BindingContext *BindingContext
	RetryContext   *RetryContext
}

type TraceContext struct {
	TraceParent string
	TraceState  string
}

type BindingContext struct {
	BindingData map[string]interface{}
}

type RetryContext struct {
	RetryCount    int
	MaxRetryCount int
}
