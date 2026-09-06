# beanstalkd-pi

A Go reimplementation of the beanstalkd work queue.

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

### Configuring a Persistence Backend

Set a persistence adapter before calling `Server.Run()`:

```go
srv, _ := NewServer(":11300")
srv.SetPersistence(&MyCustomAdapter{})
srv.Run()
```

### Implementing a Custom Adapter

1. Create a new file (e.g., `persistence_sqlite.go`)
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
