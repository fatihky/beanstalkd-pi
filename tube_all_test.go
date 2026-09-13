package main

// Tests for "list-tubes-paused" and "stats-tube-all", beanstalkd-pi
// extensions not present in stock beanstalkd. Like the rest of the
// suite, these drive the real TCP wire protocol via the harness in
// compat_harness_test.go. See protocol.txt's "Extension Commands"
// section for the exact reply formats.

import (
	"strings"
	"testing"
	"time"
)

func TestListTubesPausedEmptyByDefault(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("use work")
	c.expect("USING work")

	c.send("list-tubes-paused")
	body := c.readOK()
	if body != "---\n" {
		t.Fatalf("expected empty list, got:\n%s", body)
	}
}

func TestListTubesPausedReportsPausedTube(t *testing.T) {
	addr := startTestServer(t)
	// Two connections so "work" stays alive (current-using > 0) once the
	// admin connection moves on to inspect it - a single connection
	// switching "use" away from a tube with no jobs/watchers would GC it.
	worker := dial(t, addr)
	worker.send("use work")
	worker.expect("USING work")

	admin := dial(t, addr)
	admin.send("pause-tube work 60")
	admin.expect("PAUSED")

	admin.send("list-tubes-paused")
	body := admin.readOK()
	if body != "---\n- work\n" {
		t.Fatalf("expected only \"work\" listed, got:\n%s", body)
	}
}

func TestListTubesPausedSortedAlphabetically(t *testing.T) {
	addr := startTestServer(t)

	admin := dial(t, addr)
	for _, name := range []string{"zeta", "alpha", "mid"} {
		// One connection per tube, kept open, so each stays alive.
		worker := dial(t, addr)
		worker.send("use " + name)
		worker.expect("USING " + name)

		admin.send("pause-tube " + name + " 60")
		admin.expect("PAUSED")
	}

	admin.send("list-tubes-paused")
	body := admin.readOK()
	if body != "---\n- alpha\n- mid\n- zeta\n" {
		t.Fatalf("expected sorted list, got:\n%s", body)
	}
}

func TestListTubesPausedOmitsExpiredPause(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("use work")
	c.expect("USING work")
	c.send("pause-tube work 1")
	c.expect("PAUSED")

	time.Sleep(1200 * time.Millisecond)

	c.send("list-tubes-paused")
	body := c.readOK()
	if body != "---\n" {
		t.Fatalf("expected pause to have expired, got:\n%s", body)
	}
}

func TestListTubesPausedCountedInStats(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("list-tubes-paused")
	c.readOK()
	c.send("list-tubes-paused")
	c.readOK()

	c.send("stats")
	body := c.readOK()
	if !strings.Contains(body, "cmd-list-tubes-paused: 2\n") {
		t.Errorf("expected stats to contain \"cmd-list-tubes-paused: 2\", got:\n%s", body)
	}
}

func TestStatsTubeAllIncludesEveryTube(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("use work")
	c.expect("USING work")
	c.sendJob("put 5 0 60 3", "abc")
	c.readLine()

	c.send("stats-tube-all")
	body := c.readOK()

	if !strings.Contains(body, "- name: default\n") {
		t.Errorf("expected default tube in stats-tube-all, got:\n%s", body)
	}
	if !strings.Contains(body, "- name: work\n") {
		t.Errorf("expected work tube in stats-tube-all, got:\n%s", body)
	}
	if !strings.Contains(body, "current-jobs-ready: 1\n") {
		t.Errorf("expected work tube's ready count to show up, got:\n%s", body)
	}
}

func TestStatsTubeAllOrderedDefaultFirstThenAlphabetical(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	for _, name := range []string{"zeta", "alpha"} {
		// One connection per tube, kept open, so each stays alive
		// instead of being GC'd when the shared connection moves on.
		worker := dial(t, addr)
		worker.send("use " + name)
		worker.expect("USING " + name)
	}

	c.send("stats-tube-all")
	body := c.readOK()

	iDefault := strings.Index(body, "- name: default\n")
	iAlpha := strings.Index(body, "- name: alpha\n")
	iZeta := strings.Index(body, "- name: zeta\n")
	if iDefault < 0 || iAlpha < 0 || iZeta < 0 {
		t.Fatalf("expected all three tubes present, got:\n%s", body)
	}
	if !(iDefault < iAlpha && iAlpha < iZeta) {
		t.Fatalf("expected order default, alpha, zeta, got:\n%s", body)
	}
}

func TestStatsTubeAllMatchesStatsTubeFields(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("use work")
	c.expect("USING work")
	c.send("pause-tube work 60")
	c.expect("PAUSED")

	c.send("stats-tube work")
	single := c.readOK()

	c.send("stats-tube-all")
	all := c.readOK()

	for _, key := range []string{"pause: 60", "dlq-max-attempts: 0", "dlq-tube: "} {
		if !strings.Contains(single, key) {
			t.Fatalf("stats-tube missing %q:\n%s", key, single)
		}
		if !strings.Contains(all, "  "+key) {
			t.Fatalf("stats-tube-all missing %q:\n%s", key, all)
		}
	}
}

func TestStatsTubeAllCountedInStats(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("stats-tube-all")
	c.readOK()
	c.send("stats-tube-all")
	c.readOK()

	c.send("stats")
	body := c.readOK()
	if !strings.Contains(body, "cmd-stats-tube-all: 2\n") {
		t.Errorf("expected stats to contain \"cmd-stats-tube-all: 2\", got:\n%s", body)
	}
	if strings.Contains(body, "cmd-stats-tube: 2\n") {
		t.Errorf("expected cmd-stats-tube to be unaffected by stats-tube-all, got:\n%s", body)
	}
}

func TestListTubesPausedAndStatsTubeAllInCapabilities(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("capabilities")
	body := c.readOK()

	for _, cmd := range []string{"list-tubes-paused", "stats-tube-all"} {
		if !strings.Contains(body, "- "+cmd+"\n") {
			t.Errorf("expected extensions list to contain %q, got:\n%s", cmd, body)
		}
	}
}
