package main

// Tests for the SQLite persistence adapter's own contract - independent
// of the wire protocol harness used by the rest of the suite, these talk
// to SQLitePersistence directly.

import (
	"path/filepath"
	"strings"
	"testing"
)

// TestSqlitePersistenceRoundTripsDeadLetterField checks that a job's
// DeadLetteredFrom (see checkDeadLetter in server.go) survives a
// StoreJob/LoadAllJobs round trip.
func TestSqlitePersistenceRoundTripsDeadLetterField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")

	p := NewSQLitePersistence(path)
	if err := p.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}

	j := &PersistedJob{
		ID:               1,
		State:            StateBuried,
		TubeName:         "dead",
		DeadLetteredFrom: "default",
		Body:             []byte("abc"),
	}
	if err := p.StoreJob(j); err != nil {
		t.Fatalf("StoreJob: %v", err)
	}
	if err := p.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	jobs, err := p.LoadAllJobs()
	if err != nil {
		t.Fatalf("LoadAllJobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
	if jobs[0].DeadLetteredFrom != "default" {
		t.Fatalf("expected DeadLetteredFrom %q, got %q", "default", jobs[0].DeadLetteredFrom)
	}
	if jobs[0].TubeName != "dead" {
		t.Fatalf("expected TubeName %q, got %q", "dead", jobs[0].TubeName)
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestSqlitePersistenceMigrationIsIdempotent checks that the additive
// dead_lettered_from migration (sqliteMigrations) can run again against a
// database that already has the column, as happens on every restart
// against an existing db file - it must not error.
func TestSqlitePersistenceMigrationIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")

	p1 := NewSQLitePersistence(path)
	if err := p1.Init(); err != nil {
		t.Fatalf("first Init: %v", err)
	}
	if err := p1.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	p2 := NewSQLitePersistence(path)
	if err := p2.Init(); err != nil {
		t.Fatalf("second Init (re-running migration) should not error: %v", err)
	}
	if err := p2.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// runTestServer starts a real Server (not through startTestServer, which
// only supports nil persistence) against persist and wires up its accept
// and tick loops. Returns it and its listen address; the caller is
// responsible for shutting it down (close(srv.closeCh), srv.listener.Close(),
// persist.Close()) - unlike startTestServer, this does not register a
// t.Cleanup, since a restart test needs to shut the first instance down
// mid-test rather than at the end.
func runTestServer(t *testing.T, persist Persistence) (*Server, string) {
	t.Helper()
	srv, err := NewServer("127.0.0.1:0", persist)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	go srv.tickLoop()
	go func() {
		for {
			nc, err := srv.listener.Accept()
			if err != nil {
				return
			}
			srv.wg.Add(1)
			go srv.handleConn(nc)
		}
	}()
	return srv, srv.listener.Addr().String()
}

// TestServerRecoversDeadLetterFieldAcrossRestart is an end-to-end version
// of TestSqlitePersistenceRoundTripsDeadLetterField: it dead-letters a
// job against a live server backed by SQLite persistence, restarts the
// server against the same database file, and checks the recovered job
// still reports where it was dead-lettered from.
func TestServerRecoversDeadLetterFieldAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")

	persist1 := NewSQLitePersistence(path)
	srv1, addr1 := runTestServer(t, persist1)

	c := dial(t, addr1)
	c.send("set-dlq default 1 dead")
	c.expect("DLQ_SET")
	id := putJob(t, c, "put 10 0 60 3", "abc")
	c.send("reserve-job " + id)
	c.expect("RESERVED " + id + " 3")
	c.readBody(3)
	c.send("release " + id + " 10 0")
	c.expect("RELEASED")

	if got := tubeBuried(t, c, "dead"); got != 1 {
		t.Fatalf("expected 1 buried in dead tube before restart, got %d", got)
	}

	// Simulate a clean shutdown (mirrors the SIGTERM path in Server.Run)
	// without tearing down persist1 twice via t.Cleanup.
	close(srv1.closeCh)
	srv1.listener.Close()
	if err := persist1.Close(); err != nil {
		t.Fatalf("persist1.Close: %v", err)
	}

	persist2 := NewSQLitePersistence(path)
	srv2, addr2 := runTestServer(t, persist2)
	t.Cleanup(func() {
		close(srv2.closeCh)
		srv2.listener.Close()
		persist2.Close()
	})

	c2 := dial(t, addr2)
	c2.send("stats-job " + id)
	body := c2.readOK()
	if !strings.Contains(body, "dlq-from-tube: default\n") {
		t.Fatalf("expected recovered job to keep dlq-from-tube: default\n%s", body)
	}
}
