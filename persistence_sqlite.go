package main

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// SQLitePersistence is a durable Persistence adapter backed by SQLite
// (github.com/mattn/go-sqlite3), tuned for SSD longevity:
//
//  1. PRAGMA journal_mode=WAL — replaces the default rollback journal's
//     read-modify-delete cycle with sequential appends to a WAL file.
//  2. PRAGMA synchronous=NORMAL — fsyncs at WAL checkpoints instead of on
//     every commit, while WAL mode still keeps the database consistent.
//  3. PRAGMA temp_store=MEMORY — keeps temporary tables/indexes off disk.
//  4. Writes are buffered and flushed as a single batched transaction
//     (by count or by interval) instead of one transaction per job event,
//     cutting the number of disk writes by roughly 90-99%.
//
// A single-connection pool is used so batched writes are always applied by
// one writer, matching the batching strategy and avoiding SQLITE_BUSY
// contention under WAL.
type SQLitePersistence struct {
	path          string
	flushInterval time.Duration
	batchSize     int

	db         *sql.DB
	insertStmt *sql.Stmt
	deleteStmt *sql.Stmt

	mu      sync.Mutex
	pending []pendingOp
	closed  bool

	flushCh chan struct{}
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

type sqliteOpKind byte

const (
	sqliteOpStore sqliteOpKind = iota
	sqliteOpDelete
)

type pendingOp struct {
	kind sqliteOpKind
	job  *PersistedJob // set when kind == sqliteOpStore
	id   uint64        // set when kind == sqliteOpDelete
}

// NewSQLitePersistence creates a SQLite-backed persistence adapter that
// stores its database at path. Call Init() before use.
//
// Writes are batched: up to batchSize operations, or flushInterval -
// whichever comes first - are grouped into one transaction. This trades a
// small durability window (unflushed writes are lost on a hard crash, not
// on a clean shutdown - Close always flushes) for a large reduction in
// write amplification.
func NewSQLitePersistence(path string) *SQLitePersistence {
	return &SQLitePersistence{
		path:          path,
		flushInterval: 5 * time.Second,
		batchSize:     1000,
		flushCh:       make(chan struct{}, 1),
		stopCh:        make(chan struct{}),
	}
}

const sqliteSchema = `
CREATE TABLE IF NOT EXISTS jobs (
	id          INTEGER PRIMARY KEY,
	pri         INTEGER NOT NULL,
	delay_ns    INTEGER NOT NULL,
	ttr_ns      INTEGER NOT NULL,
	body_size   INTEGER NOT NULL,
	created_at  INTEGER NOT NULL,
	deadline_at INTEGER NOT NULL,
	reserve_ct  INTEGER NOT NULL,
	timeout_ct  INTEGER NOT NULL,
	release_ct  INTEGER NOT NULL,
	bury_ct     INTEGER NOT NULL,
	kick_ct     INTEGER NOT NULL,
	state       INTEGER NOT NULL,
	tube_name   TEXT NOT NULL,
	body        BLOB NOT NULL
);`

const sqliteInsertSQL = `
INSERT INTO jobs (id, pri, delay_ns, ttr_ns, body_size, created_at, deadline_at,
	reserve_ct, timeout_ct, release_ct, bury_ct, kick_ct, state, tube_name, body)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
	pri=excluded.pri, delay_ns=excluded.delay_ns, ttr_ns=excluded.ttr_ns,
	body_size=excluded.body_size, created_at=excluded.created_at,
	deadline_at=excluded.deadline_at, reserve_ct=excluded.reserve_ct,
	timeout_ct=excluded.timeout_ct, release_ct=excluded.release_ct,
	bury_ct=excluded.bury_ct, kick_ct=excluded.kick_ct, state=excluded.state,
	tube_name=excluded.tube_name, body=excluded.body`

const sqliteDeleteSQL = `DELETE FROM jobs WHERE id = ?`

// Init opens the database, applies the SSD-friendly PRAGMAs, creates the
// schema if needed, and starts the background batch-flush loop.
func (p *SQLitePersistence) Init() error {
	// busy_timeout lets the single writer connection wait out any brief
	// lock contention (e.g. from a concurrent `sqlite3` CLI reader)
	// instead of failing immediately with SQLITE_BUSY.
	dsn := fmt.Sprintf("file:%s?_journal_mode=WAL&_synchronous=NORMAL&_temp_store=MEMORY&_busy_timeout=5000", p.path)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return fmt.Errorf("sqlite open: %w", err)
	}

	// A single connection keeps every write serialized through one
	// transaction at a time, which is what the batching strategy assumes.
	db.SetMaxOpenConns(1)

	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA temp_store=MEMORY",
	} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return fmt.Errorf("sqlite %s: %w", pragma, err)
		}
	}

	if _, err := db.Exec(sqliteSchema); err != nil {
		db.Close()
		return fmt.Errorf("sqlite schema: %w", err)
	}

	insertStmt, err := db.Prepare(sqliteInsertSQL)
	if err != nil {
		db.Close()
		return fmt.Errorf("sqlite prepare insert: %w", err)
	}

	deleteStmt, err := db.Prepare(sqliteDeleteSQL)
	if err != nil {
		insertStmt.Close()
		db.Close()
		return fmt.Errorf("sqlite prepare delete: %w", err)
	}

	p.db = db
	p.insertStmt = insertStmt
	p.deleteStmt = deleteStmt

	p.wg.Add(1)
	go p.writeLoop()

	return nil
}

// writeLoop batches pending StoreJob/DeleteJob calls into transactions,
// flushing whenever the batch fills up, on a fixed interval, or on shutdown.
func (p *SQLitePersistence) writeLoop() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			return
		case <-p.flushCh:
			p.flush()
		case <-ticker.C:
			p.flush()
		}
	}
}

// StoreJob queues a job for persistence. It returns immediately; the write
// is applied to disk by the next batch flush (see NewSQLitePersistence).
func (p *SQLitePersistence) StoreJob(job *PersistedJob) error {
	p.mu.Lock()
	p.pending = append(p.pending, pendingOp{kind: sqliteOpStore, job: job})
	full := len(p.pending) >= p.batchSize
	p.mu.Unlock()

	if full {
		p.requestFlush()
	}
	return nil
}

// DeleteJob queues a delete for persistence. See StoreJob for batching.
func (p *SQLitePersistence) DeleteJob(id uint64) error {
	p.mu.Lock()
	p.pending = append(p.pending, pendingOp{kind: sqliteOpDelete, id: id})
	full := len(p.pending) >= p.batchSize
	p.mu.Unlock()

	if full {
		p.requestFlush()
	}
	return nil
}

func (p *SQLitePersistence) requestFlush() {
	select {
	case p.flushCh <- struct{}{}:
	default:
		// A flush is already pending; the queued batch will pick up
		// everything appended since.
	}
}

// flush applies all currently pending operations in a single transaction.
func (p *SQLitePersistence) flush() error {
	p.mu.Lock()
	batch := p.pending
	p.pending = nil
	p.mu.Unlock()

	if len(batch) == 0 {
		return nil
	}

	tx, err := p.db.Begin()
	if err != nil {
		return fmt.Errorf("sqlite begin: %w", err)
	}

	txInsert := tx.Stmt(p.insertStmt)
	txDelete := tx.Stmt(p.deleteStmt)

	for _, op := range batch {
		switch op.kind {
		case sqliteOpStore:
			j := op.job
			_, err = txInsert.Exec(
				int64(j.ID), int64(j.Pri), int64(j.Delay), int64(j.TTR), int64(j.BodySize),
				timeToNanos(j.CreatedAt), timeToNanos(j.DeadlineAt),
				int64(j.ReserveCt), int64(j.TimeoutCt), int64(j.ReleaseCt),
				int64(j.BuryCt), int64(j.KickCt), int64(j.State), j.TubeName, j.Body,
			)
		case sqliteOpDelete:
			_, err = txDelete.Exec(int64(op.id))
		}
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("sqlite exec: %w", err)
		}
	}

	return tx.Commit()
}

// Sync forces any buffered writes out to disk immediately.
func (p *SQLitePersistence) Sync() error {
	return p.flush()
}

// LoadAllJobs reads every persisted job back, used during startup recovery.
func (p *SQLitePersistence) LoadAllJobs() ([]*PersistedJob, error) {
	rows, err := p.db.Query(`
SELECT id, pri, delay_ns, ttr_ns, body_size, created_at, deadline_at,
	reserve_ct, timeout_ct, release_ct, bury_ct, kick_ct, state, tube_name, body
FROM jobs`)
	if err != nil {
		return nil, fmt.Errorf("sqlite load: %w", err)
	}
	defer rows.Close()

	var jobs []*PersistedJob
	for rows.Next() {
		var j PersistedJob
		var delayNs, ttrNs, createdAt, deadlineAt int64
		if err := rows.Scan(
			&j.ID, &j.Pri, &delayNs, &ttrNs, &j.BodySize, &createdAt, &deadlineAt,
			&j.ReserveCt, &j.TimeoutCt, &j.ReleaseCt, &j.BuryCt, &j.KickCt,
			&j.State, &j.TubeName, &j.Body,
		); err != nil {
			return nil, fmt.Errorf("sqlite scan: %w", err)
		}
		j.Delay = time.Duration(delayNs)
		j.TTR = time.Duration(ttrNs)
		j.CreatedAt = nanosToTime(createdAt)
		j.DeadlineAt = nanosToTime(deadlineAt)
		jobs = append(jobs, &j)
	}
	return jobs, rows.Err()
}

// Close flushes any buffered writes, checkpoints the WAL back into the main
// database file, and closes the connection.
func (p *SQLitePersistence) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.mu.Unlock()

	close(p.stopCh)
	p.wg.Wait()

	var errs []error
	if err := p.flush(); err != nil {
		errs = append(errs, err)
	}
	if err := p.insertStmt.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := p.deleteStmt.Close(); err != nil {
		errs = append(errs, err)
	}
	// TRUNCATE checkpoint folds the WAL back into the main db file and
	// resets it to zero bytes, so a clean shutdown leaves no WAL replay
	// to do on the next startup.
	if _, err := p.db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		errs = append(errs, err)
	}
	if err := p.db.Close(); err != nil {
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}

// timeToNanos converts t to Unix nanoseconds for storage, preserving the
// zero Time value (used for jobs with no deadline) as 0 rather than the
// out-of-int64-range nanosecond value year 1 would otherwise produce.
func timeToNanos(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

// nanosToTime is the inverse of timeToNanos.
func nanosToTime(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}
