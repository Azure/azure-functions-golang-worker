package sdk

type Context struct {
	funcName     string
	funcDir      string
	invocationID string
	traceContext TraceContext
	retryContext RetryContext
}

func NewContext(
	funcName, funcDir, invocationID string,
	traceContext TraceContext,
	retryContext RetryContext,
) *Context {
	return &Context{
		funcName:     funcName,
		funcDir:      funcDir,
		invocationID: invocationID,
		traceContext: traceContext,
		retryContext: retryContext,
	}
}

func (c *Context) InvocationID() string {
	return c.invocationID
}

func (c *Context) FunctionName() string {
	return c.funcName
}

func (c *Context) FunctionDirectory() string {
	return c.funcDir
}

func (c *Context) TraceContext() TraceContext {
	return c.traceContext
}

func (c *Context) RetryContext() RetryContext {
	return c.retryContext
}
