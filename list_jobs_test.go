package main

// Tests for the list-jobs command, a beanstalkd-pi extension not present in
// stock beanstalkd. Like the rest of the suite, these drive the real TCP
// wire protocol via the harness in compat_harness_test.go. See
// protocol.txt's "Extension Commands" section for how list-jobs differs
// from stock beanstalkd.

import (
	"fmt"
	"strings"
	"testing"
)

// readYAML reads an "OK <n>\r\n<data>\r\n" reply and returns <data>.
func readYAML(t *testing.T, c *testClient) string {
	t.Helper()
	line := c.readLine()
	if !strings.HasPrefix(line, "OK ") {
		t.Fatalf("expected OK, got %q", line)
	}
	var n int
	fmt.Sscanf(line, "OK %d", &n)
	return c.readBody(n)
}

func TestListJobsReadyOrder(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("use other")
	c.expect("USING other")

	lowID := putJob(t, c, "put 50 0 60 3", "low")
	hiID := putJob(t, c, "put 10 0 60 3", "hii")

	c.send("list-jobs other ready")
	body := readYAML(t, c)

	hiIdx := strings.Index(body, "id: "+hiID+"\n")
	lowIdx := strings.Index(body, "id: "+lowID+"\n")
	if hiIdx < 0 || lowIdx < 0 || hiIdx > lowIdx {
		t.Fatalf("expected lower-pri job %s before %s in priority order:\n%s", hiID, lowID, body)
	}
	if !strings.Contains(body, "pri: 10\n") || !strings.Contains(body, "pri: 50\n") {
		t.Fatalf("missing pri fields:\n%s", body)
	}
	if !strings.Contains(body, "size: 3\n") {
		t.Fatalf("missing size field:\n%s", body)
	}
	if strings.Contains(body, "time-left:") {
		t.Fatalf("ready jobs should not report time-left:\n%s", body)
	}
}

func TestListJobsDelayedOrder(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	soonID := putJob(t, c, "put 10 1 60 3", "soo")
	laterID := putJob(t, c, "put 10 10 60 3", "lat")

	c.send("list-jobs default delayed")
	body := readYAML(t, c)

	soonIdx := strings.Index(body, "id: "+soonID+"\n")
	laterIdx := strings.Index(body, "id: "+laterID+"\n")
	if soonIdx < 0 || laterIdx < 0 || soonIdx > laterIdx {
		t.Fatalf("expected soonest-deadline job %s before %s:\n%s", soonID, laterID, body)
	}
	if !strings.Contains(body, "time-left:") {
		t.Fatalf("delayed jobs should report time-left:\n%s", body)
	}
}

func TestListJobsBuriedOrder(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	var ids []string
	for range 3 {
		id := putJob(t, c, "put 10 0 60 3", "abc")
		c.send("reserve-job " + id)
		c.expect("RESERVED " + id + " 3")
		c.readBody(3)
		c.send("bury " + id + " 0")
		c.expect("BURIED")
		ids = append(ids, id)
	}

	c.send("list-jobs default buried")
	body := readYAML(t, c)

	prevIdx := -1
	for _, id := range ids {
		idx := strings.Index(body, "id: "+id+"\n")
		if idx < 0 {
			t.Fatalf("missing buried job %s:\n%s", id, body)
		}
		if idx < prevIdx {
			t.Fatalf("expected FIFO order in buried listing:\n%s", body)
		}
		prevIdx = idx
	}
}

func TestListJobsLimit(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	for range 5 {
		putJob(t, c, "put 10 0 60 3", "abc")
	}

	c.send("list-jobs default ready 2")
	body := readYAML(t, c)

	if got := strings.Count(body, "- id: "); got != 2 {
		t.Fatalf("expected 2 entries with limit 2, got %d:\n%s", got, body)
	}
}

func TestListJobsLimitCappedNotRejected(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	putJob(t, c, "put 10 0 60 3", "abc")

	// A limit above maxListJobsLimit is clamped, not BAD_FORMAT.
	c.send("list-jobs default ready 999999999")
	body := readYAML(t, c)
	if got := strings.Count(body, "- id: "); got != 1 {
		t.Fatalf("expected 1 entry, got %d:\n%s", got, body)
	}
}

func TestListJobsDefaultLimit(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	id := putJob(t, c, "put 10 0 60 3", "abc")

	c.send("list-jobs default ready")
	body := readYAML(t, c)
	if !strings.Contains(body, "id: "+id+"\n") {
		t.Fatalf("expected job %s with omitted limit:\n%s", id, body)
	}
}

func TestListJobsEmptyTube(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	// A watched (so it survives tube GC) but otherwise empty tube exists
	// and returns an empty listing, not NOT_FOUND.
	c.send("watch other")
	c.expect("WATCHING 2")

	c.send("list-jobs other ready")
	body := readYAML(t, c)
	if strings.TrimSpace(body) != "---" {
		t.Fatalf("expected empty listing, got:\n%s", body)
	}
}

// TestListJobsGCdTubeNotFound mirrors TestPeekTubeEmpty: a tube with no
// jobs, no watchers, and no connection using it is garbage-collected as
// soon as the last thing referencing it (here, "use") lets go, so it is
// genuinely gone rather than merely empty. See gcTube in server.go.
func TestListJobsGCdTubeNotFound(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("use other")
	c.expect("USING other")
	c.send("use default")
	c.expect("USING default")

	c.send("list-jobs other ready")
	c.expect("NOT_FOUND")
}

func TestListJobsNotFound(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("list-jobs no-such-tube ready")
	c.expect("NOT_FOUND")
}

func TestListJobsBadFormat(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("list-jobs")
	c.expect("BAD_FORMAT")
	c.send("list-jobs default")
	c.expect("BAD_FORMAT")
	c.send("list-jobs default ready 1 extra")
	c.expect("BAD_FORMAT")
	c.send("list-jobs default sideways")
	c.expect("BAD_FORMAT")
	c.send("list-jobs -bad ready")
	c.expect("BAD_FORMAT")
	c.send("list-jobs default ready notanumber")
	c.expect("BAD_FORMAT")
}

func TestListJobsNonDestructive(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	id := putJob(t, c, "put 10 0 60 3", "abc")

	c.send("list-jobs default ready")
	readYAML(t, c)

	// The job is still ready and reservable after being listed.
	c.send("reserve-job " + id)
	c.expect("RESERVED " + id + " 3")
	c.readBody(3)
}

func TestListJobsCountedInStats(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("list-jobs default ready")
	readYAML(t, c)

	c.send("stats")
	body := readYAML(t, c)

	if !strings.Contains(body, "cmd-list-jobs: 1\n") {
		t.Fatalf("stats missing cmd-list-jobs: 1\n%v", strings.Split(body, "\n"))
	}
}
