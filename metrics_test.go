package main

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestHistogramObserveAndRender exercises Histogram/writeHistogram in
// isolation, without a server: bucket placement, cumulative rendering,
// and sum/count.
func TestHistogramObserveAndRender(t *testing.T) {
	var h Histogram
	h.Observe(30 * time.Millisecond)  // falls in the 0.05 bucket
	h.Observe(400 * time.Millisecond) // falls in the 0.5 bucket
	h.Observe(2 * time.Hour)          // beyond the largest bound -> +Inf only

	snap := []tubeSnapshot{{name: "default", stat: TubeStats{WaitHist: h}}}

	var sb strings.Builder
	writeHistogram(&sb, "beanstalkd_tube_ready_wait_seconds", "help text", snap,
		func(st TubeStats) Histogram { return st.WaitHist })
	out := sb.String()

	// Bucket counts are cumulative, so the two smallest observations
	// are already reflected in the 0.5s bucket, but not 0.25s or below.
	assertBucket(t, out, "0.05", 1)
	assertBucket(t, out, "0.25", 1)
	assertBucket(t, out, "0.5", 2)
	assertBucket(t, out, "3600", 2) // the 2h sample hasn't landed yet
	assertBucket(t, out, "+Inf", 3)

	if !strings.Contains(out, `beanstalkd_tube_ready_wait_seconds_count{tube="default"} 3`) {
		t.Errorf("missing/wrong _count line:\n%s", out)
	}
}

// assertBucket checks the value of one le=... series for tube "default"
// in a rendered histogram.
func assertBucket(t *testing.T, out, le string, want uint64) {
	t.Helper()
	prefix := `beanstalkd_tube_ready_wait_seconds_bucket{tube="default",le="` + le + `"} `
	idx := strings.Index(out, prefix)
	if idx < 0 {
		t.Fatalf("missing bucket le=%q in:\n%s", le, out)
	}
	rest := out[idx+len(prefix):]
	if end := strings.IndexByte(rest, '\n'); end >= 0 {
		rest = rest[:end]
	}
	if rest != strconv.FormatUint(want, 10) {
		t.Errorf("bucket le=%q: got %s, want %d", le, rest, want)
	}
}

// TestReadyWaitHistogramRecordsReserveLatency drives a real put+reserve
// over the wire protocol and checks that the resulting wait sample shows
// up in /metrics' per-tube histogram for the tube the job was reserved
// from - the end-to-end path from Tube.Stat.WaitHist through
// snapshotStats to writeMetrics.
func TestReadyWaitHistogramRecordsReserveLatency(t *testing.T) {
	srv, addr := runTestServer(t, nil)
	t.Cleanup(func() {
		close(srv.closeCh)
		srv.listener.Close()
	})

	c := dial(t, addr)
	c.sendJob("put 0 0 60 5", "hello")
	line := c.readLine()
	if !strings.HasPrefix(line, "INSERTED ") {
		t.Fatalf("expected INSERTED, got %q", line)
	}

	time.Sleep(150 * time.Millisecond)

	c.send("reserve")
	line = c.readLine()
	if !strings.HasPrefix(line, "RESERVED ") {
		t.Fatalf("expected RESERVED, got %q", line)
	}
	c.readBody(5)

	snap := srv.snapshotStats()
	var sb strings.Builder
	writeMetrics(&sb, snap)
	out := sb.String()

	if !strings.Contains(out, `beanstalkd_tube_ready_wait_seconds_count{tube="default"} 1`) {
		t.Fatalf("expected one ready-wait sample for tube \"default\", got:\n%s", out)
	}
	// The job sat ready for >=150ms, so buckets below 0.1s must still be 0.
	assertBucket(t, out, "0.05", 0)
	assertBucket(t, out, "+Inf", 1)
}
