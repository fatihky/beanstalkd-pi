package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
)

// testClient wraps a raw connection to a locally-started server so tests
// can speak the wire protocol directly, the same way any beanstalkd client
// would.
type testClient struct {
	t  *testing.T
	nc net.Conn
	r  *bufio.Reader
}

func startTestServer(t *testing.T) string {
	t.Helper()
	return startTestServerWithMax(t, defaultMaxJobSize)
}

func startTestServerWithMax(t *testing.T, maxJobSize int) string {
	t.Helper()
	srv, err := NewServer("127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	srv.maxJobSize = maxJobSize

	go srv.tickLoop()
	go func() {
		for {
			nc, err := srv.listener.Accept()
			if err != nil {
				return
			}
			srv.wg.Add(1)
			go srv.handleConn(nc)
		}
	}()
	t.Cleanup(func() {
		close(srv.closeCh)
		srv.listener.Close()
	})
	return srv.listener.Addr().String()
}

func dial(t *testing.T, addr string) *testClient {
	t.Helper()
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	cl := &testClient{t: t, nc: nc, r: bufio.NewReader(nc)}
	t.Cleanup(func() { nc.Close() })
	return cl
}

func (c *testClient) send(cmd string) {
	c.t.Helper()
	if _, err := c.nc.Write([]byte(cmd + "\r\n")); err != nil {
		c.t.Fatalf("write %q: %v", cmd, err)
	}
}

func (c *testClient) sendJob(cmd string, body string) {
	c.t.Helper()
	c.send(cmd)
	if _, err := c.nc.Write([]byte(body + "\r\n")); err != nil {
		c.t.Fatalf("write body: %v", err)
	}
}

func (c *testClient) readLine() string {
	c.t.Helper()
	c.nc.SetReadDeadline(time.Now().Add(5 * time.Second))
	line, err := c.r.ReadString('\n')
	if err != nil {
		c.t.Fatalf("readLine: %v", err)
	}
	return strings.TrimRight(line, "\r\n")
}

func (c *testClient) readBody(n int) string {
	c.t.Helper()
	buf := make([]byte, n+2)
	c.nc.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := readFull(c.r, buf); err != nil {
		c.t.Fatalf("readBody: %v", err)
	}
	return string(buf[:n])
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (c *testClient) expect(want string) {
	c.t.Helper()
	got := c.readLine()
	if got != want {
		c.t.Fatalf("expected %q, got %q", want, got)
	}
}

// ---- tests -----------------------------------------------------------------

func TestPutReserveDelete(t *testing.T) {
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

func TestPriorityOrdering(t *testing.T) {
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

func TestDelayedJobBecomesReady(t *testing.T) {
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

func TestBlockingReserveWakesOnPut(t *testing.T) {
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

func TestTTRExpiryReleasesJob(t *testing.T) {
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

func TestBuryAndKick(t *testing.T) {
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

func TestWatchIgnore(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("watch foo")
	c.expect("WATCHING 2")

	c.send("ignore default")
	c.expect("WATCHING 1")

	c.send("ignore foo")
	c.expect("NOT_IGNORED")
}

func TestPauseTube(t *testing.T) {
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

func TestStatsCommands(t *testing.T) {
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

func TestJobTooBig(t *testing.T) {
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

func TestUnknownAndBadFormat(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("bogus-command")
	c.expect("UNKNOWN_COMMAND")

	c.send("put abc 0 60 5")
	c.expect("BAD_FORMAT")
}

func TestDeadlineSoon(t *testing.T) {
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

func TestReserveJobByID(t *testing.T) {
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

func TestConnectionCloseReleasesReservedJobs(t *testing.T) {
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
