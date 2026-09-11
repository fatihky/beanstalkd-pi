package main

// Tests for the put-at command, a beanstalkd-pi extension not present in
// stock beanstalkd. Like the rest of the suite, these drive the real TCP
// wire protocol via the harness in compat_harness_test.go. See
// protocol.txt section 1 for how put-at differs from stock beanstalkd's
// "put".

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestPutAtFutureTimestampIsDelayed(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	target := time.Now().Add(2 * time.Second).Unix()
	c.sendJob(fmt.Sprintf("put-at 10 %d 60 3", target), "abc")
	line := c.readLine()
	if !strings.HasPrefix(line, "INSERTED ") {
		t.Fatalf("expected INSERTED, got %q", line)
	}
	id := strings.TrimPrefix(line, "INSERTED ")

	// Not due yet.
	c.send("reserve-with-timeout 0")
	c.expect("TIMED_OUT")

	waitUntilReservable(t, c, "RESERVED "+id+" 3", 3, 5*time.Second)
}

func TestPutAtPastTimestampIsReadyImmediately(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	past := time.Now().Add(-1 * time.Hour).Unix()
	id := putJob(t, c, fmt.Sprintf("put-at 10 %d 60 3", past), "abc")

	c.send("reserve")
	c.expect("RESERVED " + id + " 3")
	c.readBody(3)
}

func TestPutAtBadFormat(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("put-at")
	c.expect("BAD_FORMAT")
	c.send("put-at 10")
	c.expect("BAD_FORMAT")
	c.send("put-at 10 1000000000 60")
	c.expect("BAD_FORMAT")
	c.send("put-at 10 1000000000 60 3 extra")
	c.expect("BAD_FORMAT")
	c.send("put-at sideways 1000000000 60 3")
	c.expect("BAD_FORMAT")
	c.send("put-at 10 sideways 60 3")
	c.expect("BAD_FORMAT")
}

func TestPutAtJobTooBig(t *testing.T) {
	addr := startTestServerWithMax(t, 5)
	c := dial(t, addr)

	future := time.Now().Add(time.Minute).Unix()
	c.sendJob(fmt.Sprintf("put-at 10 %d 60 10", future), "0123456789")
	c.expect("JOB_TOO_BIG")

	// Connection stays usable afterward.
	c.send("ping")
	c.expect("PONG")
}

func TestPutAtCountedSeparatelyInStats(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	future := time.Now().Add(time.Minute).Unix()
	putJob(t, c, "put 10 0 60 3", "abc")
	putJob(t, c, fmt.Sprintf("put-at 10 %d 60 3", future), "def")

	c.send("stats")
	body := c.readOK()

	if !strings.Contains(body, "cmd-put: 1\n") {
		t.Fatalf("stats missing cmd-put: 1\n%v", strings.Split(body, "\n"))
	}
	if !strings.Contains(body, "cmd-put-at: 1\n") {
		t.Fatalf("stats missing cmd-put-at: 1\n%v", strings.Split(body, "\n"))
	}
}
