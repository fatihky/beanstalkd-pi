# beanstalkd-pi

A Go reimplementation of the beanstalkd work queue.

## Testing

- `beanstalkd_compat_test.go` — the beanstalkd compatibility suite. Every
  test here (named `TestCompat_*`) drives the raw wire protocol and checks
  responses against `beanstalkd/doc/protocol.txt`, the spec vendored via the
  `beanstalkd` submodule. Run just this suite with:

  ```sh
  go test -run 'TestCompat_' .
  ```

- `compat_harness_test.go` — the shared test harness (`testClient`,
  `dial`, `startTestServer`, ...) used to talk to a locally-started server
  over a real TCP connection. It has no tests of its own.

Features added on top of stock beanstalkd get their own test file(s) (and
their own naming, not `TestCompat_`), reusing the harness above rather than
being folded into the compatibility suite.

## Persistence

beanstalkd-pi supports pluggable persistence backends via a port/adapter architecture.

### Architecture

- **Port** (`persistence.go`): Defines the `Persistence` interface and `PersistedJob` type
- **Adapters**: Implement the `Persistence` interface for specific storage backends

### Persistence Interface

```go
type Persistence interface {
    Init() error
    Close() error
    StoreJob(job *PersistedJob) error
    DeleteJob(id uint64) error
    LoadAllJobs() ([]*PersistedJob, error)
    Sync() error
}
```

### Built-in Adapters

| Adapter | File | Description |
|---------|------|-------------|
| `NoopPersistence` | `persistence_noop.go` | No-op adapter; discards all writes. Default when no backend is configured. |
| `SQLitePersistence` | `persistence_sqlite.go` | [go-sqlite3](https://github.com/mattn/go-sqlite3)-backed adapter, tuned for SSD longevity. See below. |

### Configuring a Persistence Backend

Pass a persistence adapter to `NewServer` (it must be set before `Init()`/`LoadAllJobs()` run, so it's a constructor argument rather than a post-construction setter):

```go
srv, _ := NewServer(":11300", &MyCustomAdapter{})
srv.Run()
```

Passing `nil` uses `NoopPersistence`. The CLI wires this to a flag:

```sh
beanstalkd-pi -addr :11300 -db /var/lib/beanstalkd-pi/jobs.db   # SQLite persistence
beanstalkd-pi -addr :11300                                       # no persistence (default)
```

### SQLite Adapter and SSD Optimization

`SQLitePersistence` is tuned to minimize write amplification on flash storage:

- `PRAGMA journal_mode=WAL` — sequential log appends instead of the default rollback journal's read-modify-delete cycle.
- `PRAGMA synchronous=NORMAL` — fsyncs at WAL checkpoints rather than on every commit, while WAL still keeps the database consistent.
- `PRAGMA temp_store=MEMORY` — temporary tables/indexes never touch disk.
- **Batched transactions** — `StoreJob`/`DeleteJob` calls are buffered and flushed together in a single transaction, either every 1000 operations or every 5 seconds, whichever comes first (see `NewSQLitePersistence`). This is the single biggest lever: one transaction per batch instead of one per job event.
- A single-connection pool serializes all writes through one writer, matching the batching model and avoiding `SQLITE_BUSY` under WAL.
- `Close()` runs `PRAGMA wal_checkpoint(TRUNCATE)` so a clean shutdown leaves nothing in the WAL to replay on the next start.

**Trade-off:** batching means up to `flushInterval` worth of writes can be lost on a hard crash (not on a clean shutdown, which always flushes). Call `Sync()` for an explicit flush if you need a tighter durability window. Two complementary optimizations are OS/deployment concerns outside this adapter's scope: enabling TRIM (`fstrim.timer`) and mounting the data disk with `noatime`.

### Implementing a Custom Adapter

1. Create a new file (e.g., `persistence_myadapter.go`)
2. Implement all methods of the `Persistence` interface
3. Use `ToPersistedJob(*Job)` to convert in-memory jobs to the serializable form
4. Jobs are loaded at startup via `LoadAllJobs()` and re-enqueued automatically

### Persistence Hooks

The server persists job state at these points:

- **put** — after job is enqueued
- **release** — after job is re-enqueued with new priority/delay
- **bury** — after job is moved to buried list
- **kick / kick-job** — after job is moved back to ready
- **tick** — when delayed jobs become ready
- **delete** — removes job from storage

### Recovery

On startup, `LoadAllJobs()` is called after `Init()`. All returned jobs are re-enqueued into their respective tubes, and the job ID counter is advanced past the highest recovered ID.
