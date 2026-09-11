package main

// Tests for the kick-tube command, a beanstalkd-pi extension not present in
// stock beanstalkd. Like the rest of the suite, these drive the real TCP
// wire protocol via the harness in compat_harness_test.go. See
// protocol.txt section 1 for how kick-tube differs from stock beanstalkd.

import (
	"fmt"
	"strings"
	"testing"
)

// putJob puts a job into the tube in use by c and returns its id.
func putJob(t *testing.T, c *testClient, cmd string, body string) string {
	t.Helper()
	c.sendJob(cmd, body)
	line := c.readLine()
	if !strings.HasPrefix(line, "INSERTED ") {
		t.Fatalf("expected INSERTED, got %q", line)
	}
	return strings.TrimPrefix(line, "INSERTED ")
}

// tubeBuried returns the current-jobs-buried count from stats-tube.
func tubeBuried(t *testing.T, c *testClient, tube string) int {
	t.Helper()
	c.send("stats-tube " + tube)
	line := c.readLine()
	if !strings.HasPrefix(line, "OK ") {
		t.Fatalf("expected OK, got %q", line)
	}
	var n int
	fmt.Sscanf(line, "OK %d", &n)
	body := c.readBody(n)
	for _, l := range strings.Split(body, "\n") {
		if strings.HasPrefix(l, "current-jobs-buried: ") {
			var v int
			fmt.Sscanf(l, "current-jobs-buried: %d", &v)
			return v
		}
	}
	t.Fatalf("stats-tube %s has no current-jobs-buried field:\n%s", tube, body)
	return 0
}

func TestKickTubeKicksNamedTube(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	// Bury one job in the used tube and one in "other" via reserve-job
	// (which is id-specific, unlike plain reserve).
	id1 := putJob(t, c, "put 10 0 60 3", "abc")
	c.send("reserve-job " + id1)
	c.expect("RESERVED " + id1 + " 3")
	c.readBody(3)
	c.send("bury " + id1 + " 0")
	c.expect("BURIED")

	c.send("use other")
	c.expect("USING other")
	id2 := putJob(t, c, "put 10 0 60 3", "def")
	c.send("reserve-job " + id2)
	c.expect("RESERVED " + id2 + " 3")
	c.readBody(3)
	c.send("bury " + id2 + " 0")
	c.expect("BURIED")
	c.send("use default")
	c.expect("USING default")

	// Kick the named tube; its buried count drops, the used tube's does
	// not.
	c.send("kick-tube default 10")
	c.expect("KICKED 1")
	if got := tubeBuried(t, c, "default"); got != 0 {
		t.Fatalf("expected 0 buried in default, got %d", got)
	}
	if got := tubeBuried(t, c, "other"); got != 1 {
		t.Fatalf("expected 1 buried in other, got %d", got)
	}
}

func TestKickTubeRespectsBound(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	for i := 0; i < 3; i++ {
		id := putJob(t, c, "put 10 0 60 3", "abc")
		c.send("reserve-job " + id)
		c.expect("RESERVED " + id + " 3")
		c.readBody(3)
		c.send("bury " + id + " 0")
		c.expect("BURIED")
	}

	c.send("kick-tube default 2")
	c.expect("KICKED 2")
	c.send("kick-tube default 10")
	c.expect("KICKED 1")
	c.send("kick-tube default 10")
	c.expect("KICKED 0")
}

func TestKickTubeFallsBackToDelayed(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	// No buried jobs, so the bound kicks delayed ones instead.
	putJob(t, c, "put 10 5 60 3", "abc")

	c.send("kick-tube default 10")
	c.expect("KICKED 1")

	c.send("peek-delayed")
	c.expect("NOT_FOUND")
}

func TestKickTubeNotFound(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	// Never created; a nonexistent tube is NOT_FOUND, not an empty kick.
	c.send("kick-tube no-such-tube 10")
	c.expect("NOT_FOUND")
}

func TestKickTubeBadFormat(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("kick-tube")
	c.expect("BAD_FORMAT")
	c.send("kick-tube default")
	c.expect("BAD_FORMAT")
	c.send("kick-tube default 1 extra")
	c.expect("BAD_FORMAT")
	c.send("kick-tube default -1")
	c.expect("BAD_FORMAT")
	c.send("kick-tube default abc")
	c.expect("BAD_FORMAT")
	c.send("kick-tube -bad 1")
	c.expect("BAD_FORMAT")
}

func TestKickTubeCountedInStats(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	id := putJob(t, c, "put 10 0 60 3", "abc")
	c.send("reserve-job " + id)
	c.expect("RESERVED " + id + " 3")
	c.readBody(3)
	c.send("bury " + id + " 0")
	c.expect("BURIED")

	c.send("kick-tube default 10")
	c.expect("KICKED 1")

	c.send("stats")
	line := c.readLine()
	if !strings.HasPrefix(line, "OK ") {
		t.Fatalf("expected OK, got %q", line)
	}
	var n int
	fmt.Sscanf(line, "OK %d", &n)
	body := c.readBody(n)

	if !strings.Contains(body, "cmd-kick-tube: 1\n") {
		t.Fatalf("stats missing cmd-kick-tube: 1\n%v", strings.Split(body, "\n"))
	}
	if !strings.Contains(body, "cmd-kick: 0\n") {
		t.Fatalf("stats missing cmd-kick: 0\n%v", strings.Split(body, "\n"))
	}
}
