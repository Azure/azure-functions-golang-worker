package process

// PIDFileName is the artifact record used to let the runner reap a host after
// the Go test process is terminated before test cleanup can run.
const PIDFileName = ".func-host.pid"
