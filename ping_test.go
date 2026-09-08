package main

// Tests for the ping command, a beanstalkd-pi extension not present in
// stock beanstalkd. Like the rest of the suite, these drive the real TCP
// wire protocol via the harness in compat_harness_test.go. See
// protocol.txt section 1 for how ping differs from stock beanstalkd.

import (
	"fmt"
	"strings"
	"testing"
)

func TestPingRepliesPong(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	for i := 0; i < 3; i++ {
		c.send("ping")
		c.expect("PONG")
	}
}

func TestPingCountedInStats(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("ping")
	c.expect("PONG")
	c.send("ping")
	c.expect("PONG")

	c.send("stats")
	line := c.readLine()
	if !strings.HasPrefix(line, "OK ") {
		t.Fatalf("expected OK, got %q", line)
	}
	var n int
	fmt.Sscanf(line, "OK %d", &n)
	body := c.readBody(n)
	if !strings.Contains(body, "cmd-ping: 2\n") {
		t.Errorf("expected stats to contain \"cmd-ping: 2\", got:\n%s", body)
	}
}
