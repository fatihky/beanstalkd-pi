package main

// Tests for the drain command, a beanstalkd-pi extension not present in
// stock beanstalkd. Like the rest of the suite, these drive the real TCP
// wire protocol via the harness in compat_harness_test.go. See
// protocol.txt's "Extension Commands" section for the command's exact
// reply format.

import (
	"strings"
	"testing"
)

func TestDrainDefaultsToNotDraining(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("drain status")
	c.expect("NOT_DRAINING")
}

func TestDrainOnRejectsPutThenOffAllowsIt(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("drain on")
	c.expect("DRAINING")

	c.send("drain status")
	c.expect("DRAINING")

	c.sendJob("put 0 0 0 5", "hello")
	c.expect("DRAINING")

	c.send("drain off")
	c.expect("NOT_DRAINING")

	c.send("drain status")
	c.expect("NOT_DRAINING")

	c.sendJob("put 0 0 0 5", "hello")
	line := c.readLine()
	if !strings.HasPrefix(line, "INSERTED ") {
		t.Fatalf("expected INSERTED after drain off, got %q", line)
	}
}

func TestDrainOnOffAreIdempotent(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("drain on")
	c.expect("DRAINING")
	c.send("drain on")
	c.expect("DRAINING")

	c.send("drain off")
	c.expect("NOT_DRAINING")
	c.send("drain off")
	c.expect("NOT_DRAINING")
}

func TestDrainRejectsBadArgs(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("drain")
	c.expect("BAD_FORMAT")

	c.send("drain sideways")
	c.expect("BAD_FORMAT")

	c.send("drain on off")
	c.expect("BAD_FORMAT")
}

func TestDrainCountedInStats(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("drain status")
	c.expect("NOT_DRAINING")
	c.send("drain on")
	c.expect("DRAINING")
	c.send("drain off")
	c.expect("NOT_DRAINING")

	c.send("stats")
	body := c.readOK()
	if !strings.Contains(body, "cmd-drain: 3\n") {
		t.Errorf("expected stats to contain \"cmd-drain: 3\", got:\n%s", body)
	}
	if !strings.Contains(body, "draining: false\n") {
		t.Errorf("expected stats to contain \"draining: false\", got:\n%s", body)
	}
}
