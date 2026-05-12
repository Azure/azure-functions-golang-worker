package worker

import "log/slog"

// boundAttr is a bound [slog.Attr] together with the group path that was
// open at the moment the attr was attached via WithAttrs. Used by
// [logComposer] so each bound attr can be qualified with the correct
// dotted prefix when an RpcLog is rendered later, honoring slog's
// order-sensitive Group/Attr contract.
type boundAttr struct {
	attr   slog.Attr
	groups []string
}

// logComposer accumulates the interleaved With/WithGroup state issued
// by a [slog.Logger] chain. The worker's RpcLog-emitting handlers
// ([userLogHandler], [systemLogHandler]) embed one to preserve slog's
// order-sensitive semantics:
//
//   - Attrs bound via WithAttrs are remembered together with the group
//     path that was open at the time of binding, so they are rendered
//     with that prefix and no other.
//   - Subsequent WithGroup calls extend the active group stack without
//     altering already-bound attrs.
//   - Record-time inline attrs (those carried on the slog.Record passed
//     to Handle) are qualified by the full group stack at the moment
//     Handle runs.
//
// The flat (attrs, groups) slice layout the original handlers used
// applied every group as a prefix to every bound attr, which violated
// the slog contract -- attrs bound before a WithGroup ended up nested
// under that group in the output. logComposer fixes that by snapshotting
// the group stack at bind time.
//
// Copies returned by withAttrs / withGroup share no slice backing with
// the receiver, so handler chains remain safe for concurrent use.
type logComposer struct {
	// bound is every attr the chain has accumulated via WithAttrs, in
	// the order they were attached, each tagged with the group path
	// that was open when it was attached.
	bound []boundAttr
	// groups is the full group stack at the current node of the chain.
	// Used to qualify inline record attrs at Handle time.
	groups []string
}

// withAttrs returns a new composer with attrs appended to the bound
// list, each tagged with a snapshot of the current group stack. Zero
// attrs returns the receiver unchanged.
func (c logComposer) withAttrs(attrs []slog.Attr) logComposer {
	if len(attrs) == 0 {
		return c
	}
	next := logComposer{
		bound:  make([]boundAttr, 0, len(c.bound)+len(attrs)),
		groups: cloneStrings(c.groups),
	}
	next.bound = append(next.bound, c.bound...)
	snapshot := cloneStrings(c.groups)
	for _, a := range attrs {
		next.bound = append(next.bound, boundAttr{attr: a, groups: snapshot})
	}
	return next
}

// withGroup returns a new composer with name appended to the group
// stack. Empty names are ignored (the slog convention is that an empty
// WithGroup is a no-op).
func (c logComposer) withGroup(name string) logComposer {
	if name == "" {
		return c
	}
	return logComposer{
		bound:  cloneBound(c.bound),
		groups: append(cloneStrings(c.groups), name),
	}
}

func cloneStrings(s []string) []string {
	if len(s) == 0 {
		return nil
	}
	out := make([]string, len(s))
	copy(out, s)
	return out
}

func cloneBound(b []boundAttr) []boundAttr {
	if len(b) == 0 {
		return nil
	}
	out := make([]boundAttr, len(b))
	copy(out, b)
	return out
}

// qualify renders a key with the supplied group path as a dotted
// prefix. An empty path returns the key unchanged so the common
// no-group case avoids a string allocation.
func qualify(groups []string, key string) string {
	if len(groups) == 0 {
		return key
	}
	// total length = sum(group)+len(group)+key dots
	n := len(key)
	for _, g := range groups {
		n += len(g) + 1
	}
	buf := make([]byte, 0, n)
	for _, g := range groups {
		buf = append(buf, g...)
		buf = append(buf, '.')
	}
	buf = append(buf, key...)
	return string(buf)
}
