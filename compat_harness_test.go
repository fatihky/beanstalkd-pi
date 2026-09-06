package main

// Shared wire-protocol test harness.
//
// This file holds no tests itself — just the plumbing that speaks the raw
// beanstalkd protocol over a real TCP connection to a locally-started
// server. beanstalkd_compat_test.go builds on it to verify compatibility
// with stock beanstalkd; any future test file covering features added on
// top of that base (see beanstalkd_compat_test.go's package comment) should
// reuse this same harness rather than growing its own.

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
