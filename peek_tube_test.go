package main

// Tests for the peek-tube command, a beanstalkd-pi extension not present in
// stock beanstalkd. Like the rest of the suite, these drive the real TCP
// wire protocol via the harness in compat_harness_test.go. See
// protocol.txt section 1 for how peek-tube differs from stock beanstalkd.

import (
	"fmt"
	"strings"
	"testing"
)

func TestPeekTubeReadyDelayedBuried(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("use other")
	c.expect("USING other")

	readyID := putJob(t, c, "put 10 0 60 3", "rdy")

	delayedID := putJob(t, c, "put 10 5 60 3", "dly")

	buriedID := putJob(t, c, "put 10 0 60 3", "bur")
	c.send("reserve-job " + buriedID)
	c.expect("RESERVED " + buriedID + " 3")
	c.readBody(3)
	c.send("bury " + buriedID + " 0")
	c.expect("BURIED")

	c.send("use default")
	c.expect("USING default")

	// peek-tube inspects "other" without switching the used tube.
	c.send("peek-tube other ready")
	c.expect("FOUND " + readyID + " 3")
	c.readBody(3)

	c.send("peek-tube other delayed")
	c.expect("FOUND " + delayedID + " 3")
	c.readBody(3)

	c.send("peek-tube other buried")
	c.expect("FOUND " + buriedID + " 3")
	c.readBody(3)

	// The connection's used tube never moved off "default".
	c.send("list-tube-used")
	c.expect("USING default")
}

func TestPeekTubeEmpty(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("use other")
	c.expect("USING other")
	c.send("use default")
	c.expect("USING default")

	c.send("peek-tube other ready")
	c.expect("NOT_FOUND")
	c.send("peek-tube other delayed")
	c.expect("NOT_FOUND")
	c.send("peek-tube other buried")
	c.expect("NOT_FOUND")
}

func TestPeekTubeNotFound(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	// Never created; a nonexistent tube is NOT_FOUND, not an empty peek.
	c.send("peek-tube no-such-tube ready")
	c.expect("NOT_FOUND")
}

func TestPeekTubeBadFormat(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("peek-tube")
	c.expect("BAD_FORMAT")
	c.send("peek-tube default")
	c.expect("BAD_FORMAT")
	c.send("peek-tube default ready extra")
	c.expect("BAD_FORMAT")
	c.send("peek-tube default sideways")
	c.expect("BAD_FORMAT")
	c.send("peek-tube -bad ready")
	c.expect("BAD_FORMAT")
}

func TestPeekTubeCountedInStats(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	id := putJob(t, c, "put 10 0 60 3", "abc")

	c.send("peek-tube default ready")
	c.expect("FOUND " + id + " 3")
	c.readBody(3)

	c.send("stats")
	line := c.readLine()
	if !strings.HasPrefix(line, "OK ") {
		t.Fatalf("expected OK, got %q", line)
	}
	var n int
	fmt.Sscanf(line, "OK %d", &n)
	body := c.readBody(n)

	if !strings.Contains(body, "cmd-peek-tube: 1\n") {
		t.Fatalf("stats missing cmd-peek-tube: 1\n%v", strings.Split(body, "\n"))
	}
	if !strings.Contains(body, "cmd-peek-ready: 0\n") {
		t.Fatalf("stats missing cmd-peek-ready: 0\n%v", strings.Split(body, "\n"))
	}
}
