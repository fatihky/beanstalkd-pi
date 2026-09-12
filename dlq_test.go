package main

// Tests for the set-dlq command, a beanstalkd-pi extension not present in
// stock beanstalkd, and the automatic dead-letter routing it configures
// (see checkDeadLetter in server.go). Like the rest of the suite, these
// drive the real wire protocol via the harness in compat_harness_test.go.
// See protocol.txt's Extension Commands section for set-dlq's wire form.

import (
	"strings"
	"testing"
	"time"
)

func TestDlqExhaustionViaRelease(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("set-dlq default 2 dead")
	c.expect("DLQ_SET")

	id := putJob(t, c, "put 10 0 60 3", "abc")

	// First release: releases=1, under the threshold of 2 -> back to
	// default, not dead-lettered yet.
	c.send("reserve-job " + id)
	c.expect("RESERVED " + id + " 3")
	c.readBody(3)
	c.send("release " + id + " 10 0")
	c.expect("RELEASED")
	if got := tubeBuried(t, c, "default"); got != 0 {
		t.Fatalf("expected 0 buried in default after first release, got %d", got)
	}

	// Second release: releases=2 hits the threshold -> dead-lettered.
	c.send("reserve-job " + id)
	c.expect("RESERVED " + id + " 3")
	c.readBody(3)
	c.send("release " + id + " 10 0")
	c.expect("RELEASED")

	if got := tubeBuried(t, c, "dead"); got != 1 {
		t.Fatalf("expected 1 buried in dead tube, got %d", got)
	}
	if got := tubeBuried(t, c, "default"); got != 0 {
		t.Fatalf("expected 0 buried in default, got %d", got)
	}

	// No longer reservable from default.
	c.send("reserve-with-timeout 0")
	c.expect("TIMED_OUT")
}

func TestDlqExhaustionViaTtrTimeout(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("set-dlq default 1 dead")
	c.expect("DLQ_SET")

	id := putJob(t, c, "put 10 0 1 3", "abc") // ttr=1s

	c.send("reserve-job " + id)
	c.expect("RESERVED " + id + " 3")
	c.readBody(3)

	// Let the TTR expire; tick (every 100ms) should dead-letter it on its
	// next pass since max-attempts is 1.
	time.Sleep(1300 * time.Millisecond)

	if got := tubeBuried(t, c, "dead"); got != 1 {
		t.Fatalf("expected 1 buried in dead tube after TTR timeout, got %d", got)
	}

	c.send("reserve-with-timeout 0")
	c.expect("TIMED_OUT")
}

func TestDlqDisabledNeverRoutes(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	// No set-dlq call: max-attempts stays 0 (disabled), same as before
	// this feature existed.
	id := putJob(t, c, "put 10 0 60 3", "abc")
	for i := 0; i < 5; i++ {
		c.send("reserve-job " + id)
		c.expect("RESERVED " + id + " 3")
		c.readBody(3)
		c.send("release " + id + " 10 0")
		c.expect("RELEASED")
	}

	c.send("reserve-job " + id)
	c.expect("RESERVED " + id + " 3")
	c.readBody(3)
}

// A dead-tube equal to the source tube is allowed and degenerates into
// "auto-bury this tube after N failed attempts" - useful on its own,
// without a second tube.
func TestDlqSelfReferenceAutoBuries(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("set-dlq default 1 default")
	c.expect("DLQ_SET")

	id := putJob(t, c, "put 10 0 60 3", "abc")
	c.send("reserve-job " + id)
	c.expect("RESERVED " + id + " 3")
	c.readBody(3)
	c.send("release " + id + " 10 0")
	c.expect("RELEASED")

	if got := tubeBuried(t, c, "default"); got != 1 {
		t.Fatalf("expected job auto-buried in its own tube, got %d buried", got)
	}
}

// A dead-lettered job is buried, not ready, precisely so a redrive
// requires an explicit kick rather than happening automatically.
func TestDlqKickRevivesDeadLetteredJob(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("set-dlq default 1 dead")
	c.expect("DLQ_SET")

	id := putJob(t, c, "put 10 0 60 3", "abc")
	c.send("reserve-job " + id)
	c.expect("RESERVED " + id + " 3")
	c.readBody(3)
	c.send("release " + id + " 10 0")
	c.expect("RELEASED")

	c.send("kick-tube dead 10")
	c.expect("KICKED 1")

	c.send("watch dead")
	c.expect("WATCHING 2")
	c.send("reserve-with-timeout 0")
	line := c.readLine()
	if !strings.HasPrefix(line, "RESERVED "+id+" ") {
		t.Fatalf("expected the dead-lettered job reservable in dead tube, got %q", line)
	}
	c.readBody(3)
}

func TestDlqStatsTubeShowsConfig(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("set-dlq default 3 dead")
	c.expect("DLQ_SET")

	c.send("stats-tube default")
	body := c.readOK()
	if !strings.Contains(body, "dlq-max-attempts: 3\n") {
		t.Fatalf("stats-tube missing dlq-max-attempts: 3\n%s", body)
	}
	if !strings.Contains(body, "dlq-tube: dead\n") {
		t.Fatalf("stats-tube missing dlq-tube: dead\n%s", body)
	}
}

func TestDlqStatsJobShowsOrigin(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("set-dlq default 1 dead")
	c.expect("DLQ_SET")

	id := putJob(t, c, "put 10 0 60 3", "abc")
	c.send("reserve-job " + id)
	c.expect("RESERVED " + id + " 3")
	c.readBody(3)
	c.send("release " + id + " 10 0")
	c.expect("RELEASED")

	c.send("stats-job " + id)
	body := c.readOK()
	if !strings.Contains(body, "dlq-from-tube: default\n") {
		t.Fatalf("stats-job missing dlq-from-tube: default\n%s", body)
	}
	if !strings.Contains(body, "tube: dead\n") {
		t.Fatalf("stats-job job should now be reported in the dead tube\n%s", body)
	}
}

func TestSetDlqBadFormat(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("set-dlq")
	c.expect("BAD_FORMAT")
	c.send("set-dlq default")
	c.expect("BAD_FORMAT")
	c.send("set-dlq default 1")
	c.expect("BAD_FORMAT")
	c.send("set-dlq default 1 dead extra")
	c.expect("BAD_FORMAT")
	c.send("set-dlq default abc dead")
	c.expect("BAD_FORMAT")
	c.send("set-dlq default -1 dead")
	c.expect("BAD_FORMAT")
	c.send("set-dlq -bad 1 dead")
	c.expect("BAD_FORMAT")
	c.send("set-dlq default 1 -bad")
	c.expect("BAD_FORMAT")
}

// Unlike pause-tube/kick-tube/delete-tube, set-dlq's source tube is
// created lazily (like "use"), so DLQ policy can be set up before any
// producer has touched the tube. The dead-letter tube is deliberately
// not created until a job actually lands in it.
func TestSetDlqCreatesSourceTubeLazily(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("set-dlq fresh-tube 3 dead")
	c.expect("DLQ_SET")

	c.send("list-tubes")
	body := c.readOK()
	if !strings.Contains(body, "- fresh-tube\n") {
		t.Fatalf("expected fresh-tube to be lazily created by set-dlq\n%s", body)
	}
	if strings.Contains(body, "- dead\n") {
		t.Fatalf("dead tube should not exist until a job is actually routed into it\n%s", body)
	}
}

func TestDlqCountedInStats(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("set-dlq default 1 dead")
	c.expect("DLQ_SET")

	id := putJob(t, c, "put 10 0 60 3", "abc")
	c.send("reserve-job " + id)
	c.expect("RESERVED " + id + " 3")
	c.readBody(3)
	c.send("release " + id + " 10 0")
	c.expect("RELEASED")

	c.send("stats")
	body := c.readOK()
	if !strings.Contains(body, "cmd-set-dlq: 1\n") {
		t.Fatalf("stats missing cmd-set-dlq: 1\n%v", strings.Split(body, "\n"))
	}
	if !strings.Contains(body, "job-dead-lettered: 1\n") {
		t.Fatalf("stats missing job-dead-lettered: 1\n%v", strings.Split(body, "\n"))
	}
}

// max-attempts=0 clears (rather than merely ignores) a previously
// configured dead-letter tube.
func TestSetDlqZeroDisables(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("set-dlq default 1 dead")
	c.expect("DLQ_SET")
	c.send("set-dlq default 0 dead")
	c.expect("DLQ_SET")

	id := putJob(t, c, "put 10 0 60 3", "abc")
	c.send("reserve-job " + id)
	c.expect("RESERVED " + id + " 3")
	c.readBody(3)
	c.send("release " + id + " 10 0")
	c.expect("RELEASED")

	if got := tubeBuried(t, c, "default"); got != 0 {
		t.Fatalf("expected DLQ disabled by max-attempts=0, got %d buried in default", got)
	}
}
