package main

import "fmt"

// version, commit, and date are set at build time via -ldflags (see
// .goreleaser.yaml and .github/workflows/release.yml), e.g.:
//
//	go build -ldflags "-X main.version=1.2.3 -X main.commit=abc1234 -X main.date=2026-09-12T00:00:00Z"
//
// They keep these placeholder values for `go build`/`go run` during
// development, so a dev build is never mistaken for a tagged release.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

// fullVersion is the short "beanstalkd-pi-<version>" identifier used on
// the wire (the "stats" and "capabilities" replies' version: field) and
// as a prefix elsewhere. It deliberately excludes commit/date so the
// wire protocol never grows an unbounded string.
func fullVersion() string {
	return "beanstalkd-pi-" + version
}

// versionInfo is the detailed string printed by -version and logged at
// startup: the short version plus the commit and build date, for tracing
// exactly which build is running.
func versionInfo() string {
	return fmt.Sprintf("%s (commit %s, built %s)", fullVersion(), commit, date)
}
