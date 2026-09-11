package main

// Tests for the delete-tube command, a beanstalkd-pi extension not present
// in stock beanstalkd. Like the rest of the suite, these drive the real TCP
// wire protocol via the harness in compat_harness_test.go. See protocol.txt
// section 1 (and the delete-tube paragraph under Tube GC) for how
// delete-tube differs from stock beanstalkd.

import (
	"strings"
	"testing"
)

func TestDeleteTubePurgesReadyDelayedAndBuried(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("use t1")
	c.expect("USING t1")

	putJob(t, c, "put 10 0 60 3", "rdy") // ready

	putJob(t, c, "put 10 60 60 3", "dly") // delayed

	id := putJob(t, c, "put 10 0 60 3", "bur") // will be buried
	c.send("reserve-job " + id)
	c.expect("RESERVED " + id + " 3")
	c.readBody(3)
	c.send("bury " + id + " 0")
	c.expect("BURIED")

	c.send("delete-tube t1")
	c.expect("DELETED 3")

	body := c.sendOK("stats-tube t1")
	for _, want := range []string{
		"current-jobs-ready: 0\n",
		"current-jobs-delayed: 0\n",
		"current-jobs-buried: 0\n",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("stats-tube t1 missing %q:\n%s", want, body)
		}
	}
}

func TestDeleteTubeNotFound(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("delete-tube no-such-tube")
	c.expect("NOT_FOUND")
}

func TestDeleteTubeBadFormat(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("delete-tube")
	c.expect("BAD_FORMAT")
	c.send("delete-tube t1 extra")
	c.expect("BAD_FORMAT")
	c.send("delete-tube -bad")
	c.expect("BAD_FORMAT")
}

func TestDeleteTubeDropsReservedJobOnRelease(t *testing.T) {
	addr := startTestServer(t)
	producer := dial(t, addr)
	worker := dial(t, addr)

	producer.send("use t1")
	producer.expect("USING t1")
	id := putJob(t, producer, "put 10 0 60 3", "abc")

	worker.send("watch t1")
	worker.expect("WATCHING 2")
	worker.send("ignore default")
	worker.expect("WATCHING 1")
	worker.send("reserve-with-timeout 0")
	worker.expect("RESERVED " + id + " 3")
	worker.readBody(3)

	// Stop referencing t1 via "use" before deleting, so the tube's fate
	// depends only on the outstanding reserved job.
	producer.send("use default")
	producer.expect("USING default")

	producer.send("delete-tube t1")
	producer.expect("DELETED 0") // the one job is reserved elsewhere, not counted yet

	if !listTubesContains(t, producer, "t1") {
		t.Fatalf("expected t1 to still be listed while a job is reserved on it")
	}

	// The worker finishes with the job normally - RELEASED, as usual -
	// but the job is dropped instead of going back to t1's ready queue.
	worker.send("release " + id + " 0 0")
	worker.expect("RELEASED")

	producer.send("stats-job " + id)
	producer.expect("NOT_FOUND")

	// Once the worker stops watching t1 too, the tube is fully
	// unreferenced and gcTube removes it.
	worker.send("watch default")
	worker.expect("WATCHING 2")
	worker.send("ignore t1")
	worker.expect("WATCHING 1")

	if listTubesContains(t, producer, "t1") {
		t.Fatalf("expected t1 to be gone once empty and unreferenced")
	}
}

func TestDeleteTubeDropsReservedJobOnBury(t *testing.T) {
	addr := startTestServer(t)
	producer := dial(t, addr)
	worker := dial(t, addr)

	producer.send("use t1")
	producer.expect("USING t1")
	id := putJob(t, producer, "put 10 0 60 3", "abc")

	worker.send("watch t1")
	worker.expect("WATCHING 2")
	worker.send("ignore default")
	worker.expect("WATCHING 1")
	worker.send("reserve-with-timeout 0")
	worker.expect("RESERVED " + id + " 3")
	worker.readBody(3)

	producer.send("delete-tube t1")
	producer.expect("DELETED 0")

	worker.send("bury " + id + " 0")
	worker.expect("BURIED")

	producer.send("stats-job " + id)
	producer.expect("NOT_FOUND")
	if got := tubeBuried(t, producer, "t1"); got != 0 {
		t.Fatalf("expected 0 buried in t1, got %d", got)
	}
}

func TestDeleteTubeClearsOnFreshPut(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("use t1")
	c.expect("USING t1")
	putJob(t, c, "put 10 0 60 3", "old")

	c.send("delete-tube t1")
	c.expect("DELETED 1")

	// A fresh put into the same tube works exactly as it would for any
	// other tube - the tombstone left by delete-tube doesn't linger.
	id := putJob(t, c, "put 10 0 60 3", "new")
	c.send("watch t1")
	c.expect("WATCHING 2")
	c.send("reserve-with-timeout 0")
	c.expect("RESERVED " + id + " 3")
	if got := c.readBody(3); got != "new" {
		t.Fatalf("expected body %q, got %q", "new", got)
	}
}

func TestDeleteTubePurgesDefaultButKeepsIt(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	putJob(t, c, "put 10 0 60 3", "abc") // default tube

	c.send("delete-tube default")
	c.expect("DELETED 1")

	if !listTubesContains(t, c, "default") {
		t.Fatalf("expected default to remain listed after delete-tube")
	}
}

func TestDeleteTubeCountedInStats(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("use t1")
	c.expect("USING t1")
	putJob(t, c, "put 10 0 60 3", "abc")

	c.send("delete-tube t1")
	c.expect("DELETED 1")

	body := c.sendOK("stats")
	if !strings.Contains(body, "cmd-delete-tube: 1\n") {
		t.Fatalf("stats missing cmd-delete-tube: 1\n%v", strings.Split(body, "\n"))
	}
}

// sendOK sends cmd and returns the YAML body of its "OK <bytes>" reply.
func (c *testClient) sendOK(cmd string) string {
	c.t.Helper()
	c.send(cmd)
	return c.readOK()
}

// listTubesContains reports whether "list-tubes" includes name.
func listTubesContains(t *testing.T, c *testClient, name string) bool {
	t.Helper()
	body := c.sendOK("list-tubes")
	return strings.Contains(body, "- "+name+"\n")
}
