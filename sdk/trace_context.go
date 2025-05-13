package sdk

type TraceContext struct {
	traceParent string
	traceState  string
	attributes  map[string]string
}

func NewTraceContext(traceParent, traceState string, attributes map[string]string) TraceContext {
	return TraceContext{
		traceParent: traceParent,
		traceState:  traceState,
		attributes:  attributes,
	}
}

func (tc *TraceContext) TraceParent() string {
	return tc.traceParent
}

func (tc *TraceContext) TraceState() string {
	return tc.traceState
}

func (tc *TraceContext) Attributes() map[string]string {
	return tc.attributes
}
