package main

// Feature tests exercising the remaining protocol.txt surface not already
// covered by server_test.go: release, touch, peek variants, kick-job,
// list-tubes/list-tube-used/list-tubes-watched, quit, use/default-tube
// behavior, EXPECTED_CRLF, name validation, and NOT_FOUND edge cases.
//
// Any test that must wait for a time-based state transition (a delay
// elapsing, a TTR expiring, a blocking reserve timing out) uses 5 seconds
// as the put/release/reserve time value, so the whole suite exercises
// beanstalkd's core timing behavior against one consistent, comfortably
// non-flaky interval.

import (
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
)

// readOK reads an "OK <bytes>\r\n<data>\r\n" response (used by stats,
// stats-job, stats-tube, list-tubes, list-tubes-watched) and returns the
// data section.
func (c *testClient) readOK() string {
	c.t.Helper()
	line := c.readLine()
	if !strings.HasPrefix(line, "OK ") {
		c.t.Fatalf("expected OK, got %q", line)
	}
	var n int
	fmt.Sscanf(line, "OK %d", &n)
	return c.readBody(n)
}

// readLineWithin reads one line with a custom read deadline, for responses
// that may legitimately take close to (or longer than) the default 5s
// deadline baked into readLine, e.g. a blocking reserve-with-timeout.
func (c *testClient) readLineWithin(d time.Duration) string {
	c.t.Helper()
	c.nc.SetReadDeadline(time.Now().Add(d))
	line, err := c.r.ReadString('\n')
	if err != nil {
		c.t.Fatalf("readLine: %v", err)
	}
	return strings.TrimRight(line, "\r\n")
}

// waitUntilReservable polls "reserve-with-timeout 0" on c until wantLine is
// returned (reading off its bodyLen-byte body) or within elapses,
// tolerating TIMED_OUT/DEADLINE_SOON responses while waiting.
func waitUntilReservable(t *testing.T, c *testClient, wantLine string, bodyLen int, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		c.send("reserve-with-timeout 0")
		line := c.readLine()
		if line == wantLine {
			c.readBody(bodyLen)
			return
		}
		if line != "TIMED_OUT" && line != "DEADLINE_SOON" {
			t.Fatalf("unexpected response while waiting: %q", line)
		}
		if time.Now().After(deadline) {
			t.Fatalf("job never became reservable, last=%q", line)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// ---- put: delay (core feature, 5s) -----------------------------------------

func TestPutDelayBecomesReadyAfter5Seconds(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.sendJob("put 10 5 60 3", "abc")
	line := c.readLine()
	id := strings.TrimPrefix(line, "INSERTED ")

	c.send("reserve-with-timeout 0")
	c.expect("TIMED_OUT")

	waitUntilReservable(t, c, "RESERVED "+id+" 3", 3, 7*time.Second)
}

// ---- reserve / reserve-with-timeout (core feature, 5s) ----------------------

func TestReserveWithTimeoutBlocksFor5SecondsThenTimesOut(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	start := time.Now()
	c.send("reserve-with-timeout 5")
	line := c.readLineWithin(8 * time.Second)
	elapsed := time.Since(start)

	if line != "TIMED_OUT" {
		t.Fatalf("expected TIMED_OUT, got %q", line)
	}
	if elapsed < 4*time.Second || elapsed > 7*time.Second {
		t.Fatalf("expected reserve-with-timeout 5 to block ~5s, took %v", elapsed)
	}
}

// ---- ttr expiry (core feature, 5s) ------------------------------------------

func TestTTRExpiryReleasesJobAfter5Seconds(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.sendJob("put 10 0 5 3", "abc")
	c.readLine()

	c.send("reserve")
	line := c.readLine()
	if !strings.HasPrefix(line, "RESERVED ") {
		t.Fatalf("expected RESERVED, got %q", line)
	}
	id := strings.TrimPrefix(line, "RESERVED ")
	id = strings.Fields(id)[0]
	c.readBody(3)

	waitUntilReservable(t, c, "RESERVED "+id+" 3", 3, 7*time.Second)
}

func TestTTRZeroBecomesOne(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.sendJob("put 0 0 0 3", "abc")
	line := c.readLine()
	id := strings.TrimPrefix(line, "INSERTED ")

	c.send("stats-job " + id)
	data := c.readOK()
	if !strings.Contains(data, "ttr: 1\n") {
		t.Fatalf("expected ttr silently raised to 1, got:\n%s", data)
	}
}

// ---- release -----------------------------------------------------------------

func TestReleaseRequeuesImmediately(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)
	other := dial(t, addr)

	c.sendJob("put 10 0 60 3", "abc")
	line := c.readLine()
	id := strings.TrimPrefix(line, "INSERTED ")

	c.send("reserve")
	c.expect("RESERVED " + id + " 3")
	c.readBody(3)

	c.send("release " + id + " 5 0")
	c.expect("RELEASED")

	other.send("reserve-with-timeout 0")
	other.expect("RESERVED " + id + " 3")
	other.readBody(3)
}

func TestReleaseNotFoundWhenNotReserved(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("release 123456 0 0")
	c.expect("NOT_FOUND")
}

func TestReleaseWithDelayBecomesReadyAfter5Seconds(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.sendJob("put 10 0 60 3", "abc")
	line := c.readLine()
	id := strings.TrimPrefix(line, "INSERTED ")

	c.send("reserve")
	c.expect("RESERVED " + id + " 3")
	c.readBody(3)

	c.send("release " + id + " 10 5")
	c.expect("RELEASED")

	// still delayed, nothing ready yet
	c.send("reserve-with-timeout 0")
	c.expect("TIMED_OUT")

	waitUntilReservable(t, c, "RESERVED "+id+" 3", 3, 7*time.Second)
}

// ---- bury / kick-job -----------------------------------------------------

func TestBuryNotFoundWhenNotReserved(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("bury 123456 0")
	c.expect("NOT_FOUND")
}

func TestKickJobSingle(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.sendJob("put 10 0 60 3", "abc")
	line := c.readLine()
	id := strings.TrimPrefix(line, "INSERTED ")

	c.send("reserve")
	c.readLine()
	c.readBody(3)

	c.send("bury " + id + " 0")
	c.expect("BURIED")

	c.send("kick-job " + id)
	c.expect("KICKED")

	c.send("reserve")
	c.expect("RESERVED " + id + " 3")
	c.readBody(3)
}

func TestKickJobNotFound(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	// nonexistent job
	c.send("kick-job 999999")
	c.expect("NOT_FOUND")

	// a ready job isn't in a kickable state
	c.sendJob("put 0 0 60 3", "abc")
	line := c.readLine()
	id := strings.TrimPrefix(line, "INSERTED ")
	c.send("kick-job " + id)
	c.expect("NOT_FOUND")
}

// ---- touch (core feature, 5s ttr) -------------------------------------------

func TestTouchExtendsTTR(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)
	other := dial(t, addr)

	c.sendJob("put 10 0 5 3", "abc")
	line := c.readLine()
	id := strings.TrimPrefix(line, "INSERTED ")

	c.send("reserve")
	c.expect("RESERVED " + id + " 3")
	c.readBody(3)

	time.Sleep(3 * time.Second)

	c.send("touch " + id)
	c.expect("TOUCHED")

	// 6s since the original reserve now, but only 3s since the touch: had
	// the touch not extended the deadline, the job would already be back
	// in the ready queue.
	time.Sleep(3 * time.Second)
	other.send("reserve-with-timeout 0")
	other.expect("TIMED_OUT")

	// now let the touch-extended deadline (5s after the touch) elapse.
	waitUntilReservable(t, c, "RESERVED "+id+" 3", 3, 4*time.Second)
}

func TestTouchNotFoundWhenNotReserved(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("touch 123456")
	c.expect("NOT_FOUND")
}

// ---- peek ------------------------------------------------------------------

func TestPeekByID(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.sendJob("put 10 0 60 5", "hello")
	line := c.readLine()
	id := strings.TrimPrefix(line, "INSERTED ")

	c.send("peek " + id)
	c.expect("FOUND " + id + " 5")
	c.readBody(5)

	c.send("delete " + id)
	c.expect("DELETED")

	c.send("peek " + id)
	c.expect("NOT_FOUND")
}

func TestPeekReadyDelayedBuriedEmpty(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("peek-ready")
	c.expect("NOT_FOUND")
	c.send("peek-delayed")
	c.expect("NOT_FOUND")
	c.send("peek-buried")
	c.expect("NOT_FOUND")
}

func TestPeekReadyAndDelayed(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.sendJob("put 10 0 60 3", "rdy")
	line := c.readLine()
	readyID := strings.TrimPrefix(line, "INSERTED ")

	c.sendJob("put 10 5 60 3", "dly")
	line = c.readLine()
	delayedID := strings.TrimPrefix(line, "INSERTED ")

	c.send("peek-ready")
	c.expect("FOUND " + readyID + " 3")
	c.readBody(3)

	c.send("peek-delayed")
	c.expect("FOUND " + delayedID + " 3")
	c.readBody(3)
}

// ---- use / list-tube-used / list-tubes / list-tubes-watched -----------------

func TestUseDefaultTubeThenSwitch(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("list-tube-used")
	c.expect("USING default")

	c.sendJob("put 0 0 60 3", "abc")
	line := c.readLine()
	id := strings.TrimPrefix(line, "INSERTED ")
	c.send("stats-job " + id)
	data := c.readOK()
	if !strings.Contains(data, "tube: default\n") {
		t.Fatalf("expected job in default tube, got:\n%s", data)
	}

	c.send("use work")
	c.expect("USING work")

	c.sendJob("put 0 0 60 3", "xyz")
	line = c.readLine()
	id2 := strings.TrimPrefix(line, "INSERTED ")
	c.send("stats-job " + id2)
	data = c.readOK()
	if !strings.Contains(data, "tube: work\n") {
		t.Fatalf("expected job in work tube, got:\n%s", data)
	}
}

func TestListTubes(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("use alpha")
	c.expect("USING alpha")
	c.send("watch beta")
	c.expect("WATCHING 2")

	c.send("list-tubes")
	data := c.readOK()
	for _, want := range []string{"- default\n", "- alpha\n", "- beta\n"} {
		if !strings.Contains(data, want) {
			t.Fatalf("expected list-tubes to contain %q, got:\n%s", want, data)
		}
	}
}

func TestListTubeUsed(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("use work")
	c.expect("USING work")

	c.send("list-tube-used")
	c.expect("USING work")
}

func TestListTubesWatched(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("watch foo")
	c.expect("WATCHING 2")
	c.send("watch bar")
	c.expect("WATCHING 3")

	c.send("list-tubes-watched")
	data := c.readOK()
	for _, want := range []string{"- default\n", "- foo\n", "- bar\n"} {
		if !strings.Contains(data, want) {
			t.Fatalf("expected list-tubes-watched to contain %q, got:\n%s", want, data)
		}
	}
}

func TestWatchSameTubeTwiceCountUnchanged(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("watch foo")
	c.expect("WATCHING 2")

	c.send("watch foo")
	c.expect("WATCHING 2")
}

// ---- quit --------------------------------------------------------------

func TestQuitClosesConnection(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("quit")

	c.nc.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	n, err := c.nc.Read(buf)
	if err != io.EOF || n != 0 {
		t.Fatalf("expected EOF after quit, got n=%d err=%v", n, err)
	}
}

// ---- malformed input / validation -------------------------------------------

func TestExpectedCRLF(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("put 0 0 60 3")
	if _, err := c.nc.Write([]byte("abcXY")); err != nil {
		t.Fatalf("write body: %v", err)
	}
	c.expect("EXPECTED_CRLF")
}

func TestInvalidTubeNameBadFormat(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("use -bad")
	c.expect("BAD_FORMAT")

	c.send("watch " + strings.Repeat("x", 201))
	c.expect("BAD_FORMAT")
}
