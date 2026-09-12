package main

import (
	"fmt"
	"io"
	"strconv"
	"time"
)

// waitBucketBounds are the upper bounds, in seconds, of the buckets used
// by Histogram for per-tube ready-wait observations (see
// Tube.Stat.WaitHist). They span from sub-second reservation latency up
// through jobs that sit ready for the better part of a day because
// nothing is watching the tube - the range operators actually page on,
// per stats-job's per-job "age" having no aggregate equivalent.
var waitBucketBounds = [...]float64{
	0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900, 3600, 21600, 86400,
}

// Histogram is a fixed-bucket, Prometheus-style histogram: each Observe
// increments exactly one bucket counter (found by linear scan - the
// bucket count is small enough that this beats a binary search), and
// writeHistogram turns the per-bucket counts into the cumulative
// "_bucket{le=...}" series Prometheus expects at render time. It's a
// plain value type (no pointers), so it can be embedded in TubeStats and
// copied by value into statsSnapshot under Server.mu, same as every
// other counter there.
type Histogram struct {
	buckets [len(waitBucketBounds) + 1]uint64 // last slot is the +Inf overflow bucket
	sum     float64
	count   uint64
}

// Observe records d, in seconds, into the appropriate bucket.
func (h *Histogram) Observe(d time.Duration) {
	v := d.Seconds()
	if v < 0 {
		v = 0
	}
	h.sum += v
	h.count++

	for i, bound := range waitBucketBounds {
		if v <= bound {
			h.buckets[i]++
			return
		}
	}
	h.buckets[len(waitBucketBounds)]++
}

// writeHistogram renders one Prometheus histogram series, labeled by
// tube, for every tube in tubes. get extracts the relevant Histogram
// from each tube's stats.
func writeHistogram(w io.Writer, name, help string, tubes []tubeSnapshot, get func(TubeStats) Histogram) {
	fmt.Fprintf(w, "# HELP %s %s\n", name, help)
	fmt.Fprintf(w, "# TYPE %s histogram\n", name)
	for _, t := range tubes {
		h := get(t.stat)
		var cumulative uint64
		for i, bound := range waitBucketBounds {
			cumulative += h.buckets[i]
			fmt.Fprintf(w, "%s_bucket{tube=%q,le=%q} %d\n", name, t.name, formatBound(bound), cumulative)
		}
		cumulative += h.buckets[len(waitBucketBounds)]
		fmt.Fprintf(w, "%s_bucket{tube=%q,le=\"+Inf\"} %d\n", name, t.name, cumulative)
		fmt.Fprintf(w, "%s_sum{tube=%q} %s\n", name, t.name, formatBound(h.sum))
		fmt.Fprintf(w, "%s_count{tube=%q} %d\n", name, t.name, h.count)
	}
}

// formatBound renders a bucket bound/sum the way Prometheus text
// exposition expects float values: the shortest decimal that round-trips,
// no trailing zeros.
func formatBound(f float64) string {
	return strconv.FormatFloat(f, 'g', -1, 64)
}
