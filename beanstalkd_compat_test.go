package main

// Beanstalkd compatibility suite.
//
// Every test in this file exists to verify one thing: that this server is a
// faithful, wire-compatible reimplementation of stock beanstalkd, as
// specified by beanstalkd/doc/protocol.txt (the upstream beanstalkd
// submodule vendored under beanstalkd/). Each test drives the real TCP wire
// protocol via the harness in compat_harness_test.go, the same way any
// beanstalkd client would, and checks the exact responses protocol.txt
// documents — nothing here should depend on behavior beanstalkd itself does
// not define.
//
// All test names are prefixed TestCompat_ so the suite can be run in
// isolation (`go test -run TestCompat_`) and so it stays visibly distinct
// from tests covering features layered on top of this base. As the project
// grows features beyond stock beanstalkd, those get their own test file(s)
// (and their own naming, not the TestCompat_ prefix) rather than being added
// here.
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

// ---- put / reserve / delete --------------------------------------------

func TestCompat_PutReserveDelete(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.sendJob("put 10 0 60 5", "hello")
	line := c.readLine()
	if !strings.HasPrefix(line, "INSERTED ") {
		t.Fatalf("expected INSERTED, got %q", line)
	}
	id := strings.TrimPrefix(line, "INSERTED ")

	c.send("reserve")
	line = c.readLine()
	if line != fmt.Sprintf("RESERVED %s 5", id) {
		t.Fatalf("expected RESERVED %s 5, got %q", id, line)
	}
	body := c.readBody(5)
	if body != "hello" {
		t.Fatalf("expected body 'hello', got %q", body)
	}

	c.send("delete " + id)
	c.expect("DELETED")

	c.send("delete " + id)
	c.expect("NOT_FOUND")
}

func TestCompat_PriorityOrdering(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.sendJob("put 100 0 60 1", "a")
	c.readLine()
	c.sendJob("put 5 0 60 1", "b")
	c.readLine()

	c.send("reserve")
	line := c.readLine()
	if !strings.Contains(line, " 1") { // bytes=1
		t.Fatalf("unexpected reserve line %q", line)
	}
	body := c.readBody(1)
	if body != "b" {
		t.Fatalf("expected job with lower priority first, got %q", body)
	}
}

func TestCompat_DelayedJobBecomesReady(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.sendJob("put 10 1 60 3", "abc")
	c.readLine() // INSERTED

	c.send("reserve-with-timeout 0")
	c.expect("TIMED_OUT")

	time.Sleep(1300 * time.Millisecond)

	c.send("reserve-with-timeout 1")
	line := c.readLine()
	if !strings.HasPrefix(line, "RESERVED ") {
		t.Fatalf("expected RESERVED after delay elapsed, got %q", line)
	}
	c.readBody(3)
}

func TestCompat_PutDelayBecomesReadyAfter5Seconds(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.sendJob("put 10 5 60 3", "abc")
	line := c.readLine()
	id := strings.TrimPrefix(line, "INSERTED ")

	c.send("reserve-with-timeout 0")
	c.expect("TIMED_OUT")

	waitUntilReservable(t, c, "RESERVED "+id+" 3", 3, 7*time.Second)
}

func TestCompat_BlockingReserveWakesOnPut(t *testing.T) {
	addr := startTestServer(t)
	consumer := dial(t, addr)
	producer := dial(t, addr)

	done := make(chan string, 1)
	go func() {
		consumer.send("reserve")
		line := consumer.readLine()
		done <- line
	}()

	time.Sleep(100 * time.Millisecond) // let the consumer block in reserve
	producer.sendJob("put 10 0 60 4", "ping")
	line := producer.readLine()
	if !strings.HasPrefix(line, "INSERTED ") {
		t.Fatalf("expected INSERTED, got %q", line)
	}

	select {
	case line := <-done:
		if !strings.HasPrefix(line, "RESERVED ") {
			t.Fatalf("expected RESERVED, got %q", line)
		}
		consumer.readBody(4)
	case <-time.After(3 * time.Second):
		t.Fatal("blocked reserve was never woken by put")
	}
}

func TestCompat_ReserveWithTimeoutBlocksFor5SecondsThenTimesOut(t *testing.T) {
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

func TestCompat_ReserveJobByID(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)
	other := dial(t, addr)

	c.sendJob("put 10 0 60 3", "abc")
	line := c.readLine()
	id := strings.TrimPrefix(line, "INSERTED ")

	c.send("reserve-job " + id)
	c.expect("RESERVED " + id + " 3")
	c.readBody(3)

	// already reserved by c -> NOT_FOUND for anyone else
	other.send("reserve-job " + id)
	other.expect("NOT_FOUND")

	c.send("bury " + id + " 0")
	c.expect("BURIED")

	// reserve-job can pull a buried job straight out, bypassing kick
	other.send("reserve-job " + id)
	other.expect("RESERVED " + id + " 3")
	other.readBody(3)
}

func TestCompat_ConnectionCloseReleasesReservedJobs(t *testing.T) {
	addr := startTestServer(t)
	c1 := dial(t, addr)

	c1.sendJob("put 10 0 60 3", "abc")
	line := c1.readLine()
	id := strings.TrimPrefix(line, "INSERTED ")

	c1.send("reserve")
	c1.expect("RESERVED " + id + " 3")
	c1.readBody(3)

	c1.nc.Close()

	c2 := dial(t, addr)
	deadline := time.Now().Add(2 * time.Second)
	for {
		c2.send("reserve-with-timeout 0")
		line = c2.readLine()
		if strings.HasPrefix(line, "RESERVED ") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("job never became reservable after owning connection closed, last=%q", line)
		}
		time.Sleep(20 * time.Millisecond)
	}
	c2.readBody(3)
}

// ---- ttr / touch / deadline-soon ----------------------------------------

func TestCompat_TTRExpiryReleasesJob(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.sendJob("put 10 0 1 3", "abc")
	c.readLine()

	c.send("reserve")
	line := c.readLine()
	if !strings.HasPrefix(line, "RESERVED ") {
		t.Fatalf("expected RESERVED, got %q", line)
	}
	c.readBody(3)

	time.Sleep(1300 * time.Millisecond)

	c.send("reserve-with-timeout 1")
	line = c.readLine()
	if !strings.HasPrefix(line, "RESERVED ") {
		t.Fatalf("expected job back in ready queue after ttr expiry, got %q", line)
	}
	c.readBody(3)
}

func TestCompat_TTRExpiryReleasesJobAfter5Seconds(t *testing.T) {
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

func TestCompat_TTRZeroBecomesOne(t *testing.T) {
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

func TestCompat_DeadlineSoon(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.sendJob("put 10 0 1 3", "abc")
	c.readLine()

	c.send("reserve")
	line := c.readLine()
	if !strings.HasPrefix(line, "RESERVED ") {
		t.Fatalf("expected RESERVED, got %q", line)
	}
	c.readBody(3)

	// ttr=1s, so within the last second (which starts immediately) a new
	// reserve on this connection must return DEADLINE_SOON rather than
	// block or return another job.
	c.send("reserve-with-timeout 5")
	c.expect("DEADLINE_SOON")
}

func TestCompat_TouchExtendsTTR(t *testing.T) {
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

func TestCompat_TouchNotFoundWhenNotReserved(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("touch 123456")
	c.expect("NOT_FOUND")
}

// ---- release --------------------------------------------------------------

func TestCompat_ReleaseRequeuesImmediately(t *testing.T) {
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

func TestCompat_ReleaseNotFoundWhenNotReserved(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("release 123456 0 0")
	c.expect("NOT_FOUND")
}

func TestCompat_ReleaseWithDelayBecomesReadyAfter5Seconds(t *testing.T) {
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

// ---- bury / kick / kick-job -------------------------------------------

func TestCompat_BuryAndKick(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.sendJob("put 10 0 60 3", "abc")
	line := c.readLine()
	id := strings.TrimPrefix(line, "INSERTED ")

	c.send("reserve")
	c.readLine()
	c.readBody(3)

	c.send("bury " + id + " 5")
	c.expect("BURIED")

	c.send("peek-buried")
	line = c.readLine()
	if !strings.HasPrefix(line, "FOUND "+id) {
		t.Fatalf("expected FOUND %s, got %q", id, line)
	}
	c.readBody(3)

	c.send("kick 1")
	c.expect("KICKED 1")

	c.send("reserve")
	line = c.readLine()
	if line != fmt.Sprintf("RESERVED %s 3", id) {
		t.Fatalf("expected kicked job to be reservable, got %q", line)
	}
	c.readBody(3)
}

func TestCompat_BuryNotFoundWhenNotReserved(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("bury 123456 0")
	c.expect("NOT_FOUND")
}

func TestCompat_KickJobSingle(t *testing.T) {
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

func TestCompat_KickJobNotFound(t *testing.T) {
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

// ---- peek ------------------------------------------------------------------

func TestCompat_PeekByID(t *testing.T) {
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

func TestCompat_PeekReadyDelayedBuriedEmpty(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("peek-ready")
	c.expect("NOT_FOUND")
	c.send("peek-delayed")
	c.expect("NOT_FOUND")
	c.send("peek-buried")
	c.expect("NOT_FOUND")
}

func TestCompat_PeekReadyAndDelayed(t *testing.T) {
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

// ---- watch / ignore / use / list-tube* / pause-tube ------------------------

func TestCompat_WatchIgnore(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("watch foo")
	c.expect("WATCHING 2")

	c.send("ignore default")
	c.expect("WATCHING 1")

	c.send("ignore foo")
	c.expect("NOT_IGNORED")
}

func TestCompat_WatchSameTubeTwiceCountUnchanged(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("watch foo")
	c.expect("WATCHING 2")

	c.send("watch foo")
	c.expect("WATCHING 2")
}

func TestCompat_PauseTube(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("use work")
	c.expect("USING work")
	c.send("watch work")
	c.expect("WATCHING 2")

	c.send("pause-tube work 1")
	c.expect("PAUSED")

	c.sendJob("put 10 0 60 3", "abc")
	c.readLine()

	c.send("reserve-with-timeout 0")
	c.expect("TIMED_OUT") // paused, so nothing to reserve yet

	time.Sleep(1200 * time.Millisecond)

	c.send("reserve-with-timeout 1")
	line := c.readLine()
	if !strings.HasPrefix(line, "RESERVED ") {
		t.Fatalf("expected job reservable after pause elapsed, got %q", line)
	}
	c.readBody(3)
}

func TestCompat_UseDefaultTubeThenSwitch(t *testing.T) {
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

func TestCompat_ListTubes(t *testing.T) {
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

func TestCompat_ListTubeUsed(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("use work")
	c.expect("USING work")

	c.send("list-tube-used")
	c.expect("USING work")
}

func TestCompat_ListTubesWatched(t *testing.T) {
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

// TestCompat_TubeGC checks protocol.txt's "If a tube is empty ... and no
// client refers to it, it will be deleted": a tube vanishes from list-tubes
// as soon as it has no ready/delayed/reserved/buried jobs and no connection
// uses or watches it, whether that reference drops via delete, use, ignore,
// or the connection simply going away — but "default" never does.
func TestCompat_TubeGC(t *testing.T) {
	addr := startTestServer(t)

	// use + delete: the tube survives while still in use, and disappears
	// once the using connection moves elsewhere.
	c1 := dial(t, addr)
	c1.send("use gone")
	c1.expect("USING gone")

	c1.sendJob("put 0 0 60 3", "xyz")
	line := c1.readLine()
	id := strings.TrimPrefix(line, "INSERTED ")

	c1.send("delete " + id)
	c1.expect("DELETED")

	c1.send("list-tubes")
	if got := c1.readOK(); !strings.Contains(got, "- gone\n") {
		t.Fatalf("expected list-tubes to still contain gone (still in use), got:\n%s", got)
	}

	c1.send("use default")
	c1.expect("USING default")

	c1.send("list-tubes")
	if got := c1.readOK(); strings.Contains(got, "- gone\n") {
		t.Fatalf("expected list-tubes to no longer contain gone, got:\n%s", got)
	}

	// watch + ignore: an empty, never-used tube disappears as soon as the
	// last watcher ignores it.
	c2 := dial(t, addr)
	c2.send("watch temp")
	c2.expect("WATCHING 2")

	c2.send("ignore temp")
	c2.expect("WATCHING 1")

	c2.send("list-tubes")
	if got := c2.readOK(); strings.Contains(got, "- temp\n") {
		t.Fatalf("expected list-tubes to no longer contain temp, got:\n%s", got)
	}

	// connection close: an empty tube only referenced by a connection that
	// then disconnects disappears too.
	c3 := dial(t, addr)
	c3.send("use closeme")
	c3.expect("USING closeme")
	c3.nc.Close()

	c1.send("list-tubes")
	deadline := time.Now().Add(5 * time.Second)
	for {
		got := c1.readOK()
		if !strings.Contains(got, "- closeme\n") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected list-tubes to no longer contain closeme, got:\n%s", got)
		}
		time.Sleep(50 * time.Millisecond)
		c1.send("list-tubes")
	}

	// default is never collected, even when briefly empty and unreferenced.
	c1.send("list-tubes")
	if got := c1.readOK(); !strings.Contains(got, "- default\n") {
		t.Fatalf("expected list-tubes to always contain default, got:\n%s", got)
	}
}

// ---- stats ------------------------------------------------------------------

func TestCompat_StatsCommands(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.sendJob("put 10 0 60 3", "abc")
	line := c.readLine()
	id := strings.TrimPrefix(line, "INSERTED ")

	c.send("stats-job " + id)
	line = c.readLine()
	if !strings.HasPrefix(line, "OK ") {
		t.Fatalf("expected OK, got %q", line)
	}
	n := 0
	fmt.Sscanf(line, "OK %d", &n)
	c.readBody(n)

	c.send("stats-tube default")
	line = c.readLine()
	if !strings.HasPrefix(line, "OK ") {
		t.Fatalf("expected OK, got %q", line)
	}
	fmt.Sscanf(line, "OK %d", &n)
	c.readBody(n)

	c.send("stats")
	line = c.readLine()
	if !strings.HasPrefix(line, "OK ") {
		t.Fatalf("expected OK, got %q", line)
	}
	fmt.Sscanf(line, "OK %d", &n)
	c.readBody(n)
}

// ---- quit --------------------------------------------------------------

func TestCompat_QuitClosesConnection(t *testing.T) {
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

// ---- malformed input / limits / validation ----------------------------------

func TestCompat_JobTooBig(t *testing.T) {
	addr := startTestServerWithMax(t, 10)
	c := dial(t, addr)

	c.sendJob("put 0 0 60 20", strings.Repeat("x", 20))
	c.expect("JOB_TOO_BIG")

	// the connection must still be usable afterwards (body+CRLF fully
	// consumed and the stream resynchronized).
	c.sendJob("put 0 0 60 3", "abc")
	line := c.readLine()
	if !strings.HasPrefix(line, "INSERTED ") {
		t.Fatalf("connection desynced after JOB_TOO_BIG: got %q", line)
	}
}

func TestCompat_UnknownAndBadFormat(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("bogus-command")
	c.expect("UNKNOWN_COMMAND")

	c.send("put abc 0 60 5")
	c.expect("BAD_FORMAT")
}

func TestCompat_ExpectedCRLF(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("put 0 0 60 3")
	if _, err := c.nc.Write([]byte("abcXY")); err != nil {
		t.Fatalf("write body: %v", err)
	}
	c.expect("EXPECTED_CRLF")
}

func TestCompat_InvalidTubeNameBadFormat(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("use -bad")
	c.expect("BAD_FORMAT")

	c.send("watch " + strings.Repeat("x", 201))
	c.expect("BAD_FORMAT")
}

// ---- ping (beanstalkd-pi extension, not in stock beanstalkd) -----------

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
