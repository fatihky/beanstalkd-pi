package main

// Tests for the stats-conn and list-connections commands, beanstalkd-pi
// extensions not present in stock beanstalkd. Like the rest of the suite,
// these drive the real TCP wire protocol via the harness in
// compat_harness_test.go. See protocol.txt's "Extension Commands" section
// for how these commands behave.

import (
	"strings"
	"testing"
)

func TestStatsConnSelfDescribing(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("use other")
	c.expect("USING other")

	c.send("watch foo")
	c.expect("WATCHING 2") // "other" isn't watched by "use"; default + foo

	id := putJob(t, c, "put 10 0 60 3", "abc")
	c.send("reserve-job " + id)
	c.expect("RESERVED " + id + " 3")
	c.readBody(3)

	c.send("stats-conn")
	body := c.readOK()

	if !strings.Contains(body, "tube: other\n") {
		t.Fatalf("expected tube: other, got:\n%s", body)
	}
	if !strings.Contains(body, "watching: [default, foo]\n") {
		t.Fatalf("expected watching list, got:\n%s", body)
	}
	if !strings.Contains(body, "reserved-jobs: ["+id+"]\n") {
		t.Fatalf("expected reserved-jobs list, got:\n%s", body)
	}
	if !strings.Contains(body, "producer: true\n") {
		t.Fatalf("expected producer: true, got:\n%s", body)
	}
	if !strings.Contains(body, "worker: true\n") {
		t.Fatalf("expected worker: true, got:\n%s", body)
	}
	if !strings.Contains(body, "waiting: false\n") {
		t.Fatalf("expected waiting: false, got:\n%s", body)
	}
	if !strings.Contains(body, "addr: ") {
		t.Fatalf("expected addr field, got:\n%s", body)
	}

	// The id reported must match what a second connection sees for this
	// same connection via list-connections.
	var reportedID string
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "id: ") {
			reportedID = strings.TrimPrefix(line, "id: ")
		}
	}
	if reportedID == "" {
		t.Fatalf("stats-conn reply missing id, got:\n%s", body)
	}

	c2 := dial(t, addr)
	c2.send("list-connections")
	listBody := c2.readOK()
	if !strings.Contains(listBody, "- id: "+reportedID+"\n") {
		t.Fatalf("expected list-connections to contain id: %s, got:\n%s", reportedID, listBody)
	}
	if !strings.Contains(listBody, "  reserved-jobs: ["+id+"]\n") {
		t.Fatalf("expected list-connections to show reserved job %s, got:\n%s", id, listBody)
	}
}

func TestStatsConnDefaults(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("stats-conn")
	body := c.readOK()

	if !strings.Contains(body, "tube: default\n") {
		t.Fatalf("expected tube: default, got:\n%s", body)
	}
	if !strings.Contains(body, "watching: [default]\n") {
		t.Fatalf("expected watching: [default], got:\n%s", body)
	}
	if !strings.Contains(body, "reserved-jobs: []\n") {
		t.Fatalf("expected reserved-jobs: [], got:\n%s", body)
	}
	if !strings.Contains(body, "producer: false\n") {
		t.Fatalf("expected producer: false, got:\n%s", body)
	}
	if !strings.Contains(body, "worker: false\n") {
		t.Fatalf("expected worker: false, got:\n%s", body)
	}
}

func TestStatsConnWithID(t *testing.T) {
	addr := startTestServer(t)
	c1 := dial(t, addr)
	c2 := dial(t, addr)

	c1.send("use tube-a")
	c1.expect("USING tube-a")

	// Learn c1's own connection id via its own no-arg stats-conn.
	c1.send("stats-conn")
	body1 := c1.readOK()
	var id1 string
	for _, line := range strings.Split(body1, "\n") {
		if strings.HasPrefix(line, "id: ") {
			id1 = strings.TrimPrefix(line, "id: ")
		}
	}
	if id1 == "" {
		t.Fatalf("stats-conn reply missing id, got:\n%s", body1)
	}

	// c2 asks about c1's connection by id and should see c1's stats, not
	// its own.
	c2.send("stats-conn " + id1)
	body2 := c2.readOK()
	if !strings.Contains(body2, "id: "+id1+"\n") {
		t.Fatalf("expected id: %s, got:\n%s", id1, body2)
	}
	if !strings.Contains(body2, "tube: tube-a\n") {
		t.Fatalf("expected tube: tube-a, got:\n%s", body2)
	}
}

func TestStatsConnUnknownID(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("stats-conn 999999")
	c.expect("NOT_FOUND")
}

func TestStatsConnBadFormat(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("stats-conn abc")
	c.expect("BAD_FORMAT")

	c.send("stats-conn 1 2")
	c.expect("BAD_FORMAT")
}

func TestListConnectionsMultiple(t *testing.T) {
	addr := startTestServer(t)
	c1 := dial(t, addr)
	c2 := dial(t, addr)

	c1.send("use tube-a")
	c1.expect("USING tube-a")
	c2.send("use tube-b")
	c2.expect("USING tube-b")

	c1.send("list-connections")
	body := c1.readOK()

	if !strings.Contains(body, "tube: tube-a\n") {
		t.Fatalf("expected an entry with tube: tube-a, got:\n%s", body)
	}
	if !strings.Contains(body, "tube: tube-b\n") {
		t.Fatalf("expected an entry with tube: tube-b, got:\n%s", body)
	}
}

func TestListConnectionsCountedInStats(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("list-connections")
	c.readOK()
	c.send("stats-conn")
	c.readOK()

	c.send("stats")
	body := c.readOK()

	if !strings.Contains(body, "cmd-list-connections: 1\n") {
		t.Fatalf("stats missing cmd-list-connections: 1\n%v", strings.Split(body, "\n"))
	}
	if !strings.Contains(body, "cmd-stats-conn: 1\n") {
		t.Fatalf("stats missing cmd-stats-conn: 1\n%v", strings.Split(body, "\n"))
	}
}
