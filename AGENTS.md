# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Go reimplementation of the beanstalkd work queue server. The whole
server (`main`) lives as a flat package at the repo root — there is no
internal/ or pkg/ split. `beanstalkd/` is a git submodule vendoring the
upstream C beanstalkd repo, used only as the reference spec
(`beanstalkd/doc/protocol.txt`); it is not built or imported.

Read `protocol.txt` before touching `protocol.go` or `server.go` — it
mirrors the structure of upstream beanstalkd's own protocol.txt and
documents exactly where this implementation's wire protocol matches stock
beanstalkd (same commands, arguments, and replies) and where/why it
deliberately diverges (error replies, extra commands), plus the three
extension commands with no stock equivalent. Update it when a change
affects wire compatibility or adds/changes a command.

Read `documentation.txt` before touching the persistence layer,
concurrency model, CLI flags, process lifecycle/signals, or the
observability HTTP server — it covers the application itself: build/run/
test commands, tube GC, the persistence adapter model, and the
`/metrics`/`/healthz`/pprof server. Update it when a change affects any
of those.

## Commands

Build the server:

```sh
go build -o beanstalkd-pi .
```

Run it:

```sh
go run . -addr :11300                                  # no persistence
go run . -addr :11300 -db /var/lib/beanstalkd-pi/jobs.db  # SQLite persistence
```

Other flags: `-http-addr` (observability server, default `:11301`, empty
disables it), `-pprof` (expose `net/http/pprof` on that server),
`-log-level` (debug/info/warn/error), `-log-format` (text/json),
`-slow-log-threshold` (warn on slow lock holds/waits or reserve calls).

Run the whole test suite:

```sh
go test .
```

Run just the beanstalkd compatibility suite:

```sh
go test -run 'TestCompat_' .
```

Run a single test:

```sh
go test -run TestCompat_PutReserveDelete .
```

Build/run the benchmark tool (a separate `main` package under `cmd/`):

```sh
go build -o beanstalkd-bench ./cmd/beanstalkd-bench
./beanstalkd-bench -addr 127.0.0.1:11300 -mode both -n 10000
```

## Architecture

**Concurrency model.** One goroutine per accepted connection
(`server.go:handleConn`); all shared state (tubes, the job index, global
stats, per-connection waiting/reserved lists) is guarded by a single
`Server.mu`. A separate `tickLoop` goroutine runs every 100ms
(`server.go:tick`) to promote delayed jobs to ready, expire TTRs,
fire `DEADLINE_SOON`/`TIMED_OUT`, and satisfy blocked `reserve` calls —
this is the polling equivalent of stock beanstalkd's single-threaded
libevent reactor. Lock waits/holds and reserve calls exceeding
`-slow-log-threshold` are logged as warnings (`Server.lockTraced`,
`Conn.traceReserve`).

**Connection state machine.** `Conn.serve()` (`protocol.go`) is a loop over
`Conn.state` (`StateWantCommand`, `StateWantData`, `StateBitBucket`,
`StateWait`, `StateClose`), mirroring stock beanstalkd's conn.c state
machine: read a command line, optionally read a job body (or discard one
via the bit bucket for `JOB_TOO_BIG`/`DRAINING`), or block waiting on a
`reserve`.

**Core types**, one file each:

- `job.go` — `Job` and its lifecycle states (ready/reserved/delayed/buried).
- `tube.go` — `Tube`: a ready heap, a delay heap, and a doubly-linked
  buried list (FIFO), plus per-tube stats and pause state.
- `heap.go` — generic min-heap (`Heap[T]`) used for both the ready queue
  (ordered by priority, then id) and the delay queue (ordered by
  deadline, then id); each heap element tracks its own index for O(log n)
  removal from the middle (needed by `delete`/`kick-job`/`reserve-job`).
- `index.go` — `JobIndex`: O(1) job lookup by id across all tubes.
- `conn.go` — per-connection reserved-job list and watch/use tube
  bookkeeping.
- `server.go` — `Server`: tube map, job index, global stats/counters,
  accept loop, tick loop, stats formatting (YAML for the wire protocol).

**Command dispatch.** `protocol.go:dispatchCmd` switches on the command
name and each `handleX` method validates args, takes `Server.mu`, mutates
state, and writes a reply. This is the file to extend when adding a new
beanstalk command.

**Persistence is a port/adapter, not a binlog.** `persistence.go` defines
the `Persistence` interface (`Init`, `Close`, `StoreJob`, `DeleteJob`,
`LoadAllJobs`, `Sync`) and `PersistedJob`, decoupled from the in-memory
`Job` struct. `NewServer` takes the adapter as a constructor argument
(nil → `persistence_noop.go`'s no-op) because `LoadAllJobs` must run
before the server starts accepting connections, to re-enqueue recovered
jobs and advance the id counter past the highest recovered id.
`persistence_sqlite.go` is the built-in durable adapter: WAL journal
mode, batched transactions (flushed every 1000 ops or 5s), a
single-connection writer pool, `PRAGMA wal_checkpoint(TRUNCATE)` on
`Close`. See `README.md`'s Persistence section for the full rationale
and the hook points (`put`, `release`, `bury`, `kick`/`kick-job`, tick's
delayed→ready promotion, `delete`). To add a backend, implement the
interface in a new `persistence_*.go` file and use `ToPersistedJob`.

**Observability is additive, not part of the wire protocol.**
`admin.go`/`metrics.go` run a separate `net/http` server
(`-http-addr`) exposing `/metrics` (Prometheus text format, built from a
lock-free snapshot via `Server.snapshotStats`), `/healthz`, and
optionally `/debug/pprof/*` (only when `-pprof` is passed). `log.go` sets
up the process-wide `log/slog` logger (`initLogger`); package-level code
defaults to an info-level text logger so it's never nil, including in
tests.

**Testing.** `compat_harness_test.go` holds the shared wire-protocol test
harness (`testClient`, `dial`, `startTestServer`) that drives a real TCP
connection against a locally-started server — no test logic of its own.
`beanstalkd_compat_test.go` is the compatibility suite: every
`TestCompat_*` test checks exact wire responses against
`beanstalkd/doc/protocol.txt`. Features added on top of stock beanstalkd
get their own test file(s), reusing the same harness, with names that are
deliberately _not_ prefixed `TestCompat_` so the compatibility suite stays
runnable in isolation.
