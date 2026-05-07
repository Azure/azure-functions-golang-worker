package sdk

import (
	"log/slog"
	"reflect"
)

// init auto-installs an [sdk]-flavored slog default at package import time
// so user code that calls slog.InfoContext (etc.) routes through Functions
// logging without requiring an explicit slog.SetDefault call.
//
// The install is conservative: it only happens when slog.Default is still
// the standard library's untouched default. If a user has already replaced
// the default by the time this init runs (typically by calling
// slog.SetDefault from another package's init that imported earlier), the
// user's choice wins.
//
// Detection uses reflection on the unexported *slog.defaultHandler type
// emitted by the Go standard library. This is intentionally fragile-by-
// design: any Go release that renames or restructures slog's internal
// default would cause us to skip the install, leaving the user's logging
// behavior unchanged. That is the safer failure mode than silently
// overriding a user-installed handler.
//
// Users who want full control of slog can opt out of this install by
// calling slog.SetDefault(...) anywhere in their main, after the worker
// package has been imported. The override is persistent — sdk's init runs
// only once, and worker.Start does not undo user-installed defaults.
func init() {
	if !slogDefaultIsStdlibUntouched() {
		return
	}
	slog.SetDefault(NewLogger())
}

// slogDefaultIsStdlibUntouched reports whether slog.Default is currently
// using the Go standard library's *slog.defaultHandler — i.e. the user
// has not customized logging yet.
//
// We rely on the type's reflective name "*slog.defaultHandler". A future
// Go release that renames or relocates this type will cause this check to
// return false and skip the auto-install — a safe degradation: the worst
// case is that users have to call slog.SetDefault(sdk.NewLogger())
// themselves, restoring the previous behavior of this package.
func slogDefaultIsStdlibUntouched() bool {
	h := slog.Default().Handler()
	if h == nil {
		return false
	}
	t := reflect.TypeOf(h)
	if t == nil {
		return false
	}
	// slog's internal default is *slog.defaultHandler. We check by
	// reflective string match because the type is unexported.
	return t.String() == "*slog.defaultHandler"
}
