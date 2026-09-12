package main

import "fmt"

// version, commit, and date are set at build time via -ldflags (see
// ../../.goreleaser.yaml and ../../.github/workflows/release.yml), the
// same way as the main beanstalkd-pi binary's version.go.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

// versionInfo is the string printed by -version: name, version, commit,
// and build date, for tracing exactly which build produced a benchmark
// run.
func versionInfo() string {
	return fmt.Sprintf("beanstalkd-bench-%s (commit %s, built %s)", version, commit, date)
}
