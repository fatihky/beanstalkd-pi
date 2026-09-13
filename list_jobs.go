package main

// Implements "list-jobs", a beanstalkd-pi extension with no stock
// equivalent: stock beanstalkd's only per-tube views (peek-ready/
// peek-delayed/peek-buried) return at most one job each. See
// protocol.txt's "Extension Commands" section for the wire-level
// contract and dispatchCmd in protocol.go for where this is wired into
// command dispatch.

import (
	"fmt"
	"sort"
	"strconv"
)

const (
	// defaultListJobsLimit/maxListJobsLimit bound "list-jobs": the
	// scan, sort, and YAML build all happen under Server.mu, so an
	// unbounded or client-chosen-huge limit would stall every other
	// connection and the tick loop for its duration.
	// defaultListJobsLimit applies when the client omits the optional
	// limit argument; maxListJobsLimit silently caps an explicit one
	// rather than rejecting it.
	defaultListJobsLimit = 100
	maxListJobsLimit     = 10000
)

// handleListJobs is "list-jobs <tube> <state> [limit]": a bounded,
// non-destructive listing of a tube's ready/delayed/buried jobs, sorted
// the same way the tube would serve them (ready: priority then id;
// delayed: soonest deadline then id; buried: FIFO). limit defaults to
// defaultListJobsLimit and is silently capped at maxListJobsLimit -
// the whole scan/sort/format happens under Server.mu, so an unbounded
// limit on a large tube would stall every other connection. Reserved
// jobs aren't listable here: they live on each connection's reserved
// list, not a per-tube heap, so there's no equivalently cheap bounded
// scan for them. Job bodies aren't included in the reply (they can be
// arbitrary, non-YAML-safe bytes) - follow up with "peek <id>", which
// looks a job up by id regardless of state, to fetch one.
func (c *Conn) handleListJobs(args []string) {
	if len(args) != 2 && len(args) != 3 {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	name := args[0]
	if !validTubeName(name) {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	state := args[1]
	if state != "ready" && state != "delayed" && state != "buried" {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	limit := defaultListJobsLimit
	if len(args) == 3 {
		n, err := strconv.ParseUint(args[2], 10, 32)
		if err != nil {
			c.replyWord("BAD_FORMAT\r\n")
			return
		}
		limit = int(n)
	}
	if limit > maxListJobsLimit {
		limit = maxListJobsLimit
	}

	c.Server.mu.Lock()
	defer c.Server.mu.Unlock()

	c.Server.globalStats.CmdListJobs++

	t, ok := c.Server.tubes[name]
	if !ok {
		c.replyWord("NOT_FOUND\r\n")
		return
	}

	var jobs []*Job
	switch state {
	case "ready":
		jobs = t.Ready.Snapshot()
		sort.Slice(jobs, func(i, j int) bool { return jobPriLess(jobs[i], jobs[j]) })
		if len(jobs) > limit {
			jobs = jobs[:limit]
		}
	case "delayed":
		jobs = t.Delay.Snapshot()
		sort.Slice(jobs, func(i, j int) bool { return jobDelayLess(jobs[i], jobs[j]) })
		if len(jobs) > limit {
			jobs = jobs[:limit]
		}
	case "buried":
		head := t.BuriedHead
		for j := head.buriedNext; j != head && len(jobs) < limit; j = j.buriedNext {
			jobs = append(jobs, j)
		}
	}

	c.sendYAML(c.Server.formatListJobs(jobs, state))
}

// formatListJobs renders the reply for "list-jobs <tube> <state>
// [limit]": a bounded, sorted listing of a tube's ready/delayed/buried
// jobs. Deliberately lighter than stats-job's per-job YAML - this is a
// listing to find an id of interest, not a full dump; follow up with
// "peek <id>" (state-agnostic by job id) to fetch a specific job's
// body.
//
// Caller must hold s.mu.
func (s *Server) formatListJobs(jobs []*Job, state string) string {
	result := "---\n"
	for _, j := range jobs {
		result += fmt.Sprintf("- id: %d\n  pri: %d\n  age: %d\n  size: %d\n",
			j.ID, j.Pri, int64(j.age().Seconds()), j.BodySize)
		if state == "delayed" {
			result += fmt.Sprintf("  time-left: %d\n", int64(j.timeLeft().Seconds()))
		}
	}
	return result
}
