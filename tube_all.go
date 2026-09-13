package main

// Implements "list-tubes-paused" and "stats-tube-all", beanstalkd-pi
// extensions with no stock equivalent: an operator console or monitoring
// tool that wants every tube's stats today has to do list-tubes followed
// by N x stats-tube - N lock acquisitions per refresh. Both commands here
// collapse that into a single lock acquisition. See protocol.txt's
// "Extension Commands" section for the wire-level contract and
// dispatchCmd in protocol.go for where these are wired into command
// dispatch.

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// handleListTubesPaused replies with the names of every tube currently
// paused (pause-tube with time left on the clock), sparing a monitoring
// tool from list-tubes + N x stats-tube just to find out which tubes are
// paused.
func (c *Conn) handleListTubesPaused() {
	c.Server.mu.Lock()
	defer c.Server.mu.Unlock()

	c.Server.globalStats.CmdListTubesPaused++

	yaml := c.Server.formatListTubesPaused()
	c.sendYAML(yaml)
}

// formatListTubesPaused returns a YAML sequence of the names of every
// currently paused tube, sorted for deterministic output. isPaused
// clears a tube's Pause once its deadline has passed, so a tube whose
// pause just expired is correctly omitted rather than reported stale.
//
// Caller must hold s.mu.
func (s *Server) formatListTubesPaused() string {
	now := time.Now()
	names := make([]string, 0)
	for name, t := range s.tubes {
		if t.isPaused(now) {
			names = append(names, name)
		}
	}
	sort.Strings(names)

	var b strings.Builder
	b.WriteString("---\n")
	for _, name := range names {
		b.WriteString("- " + name + "\n")
	}
	return b.String()
}

// handleStatsTubeAll replies with stats-tube's fields for every tube in
// one YAML document, sparing a monitoring tool the N lock acquisitions
// list-tubes + N x stats-tube would otherwise cost per refresh.
func (c *Conn) handleStatsTubeAll() {
	c.Server.mu.Lock()
	defer c.Server.mu.Unlock()

	c.Server.globalStats.CmdStatsTubeAll++

	yaml := c.Server.formatAllTubeStats()
	c.sendYAML(yaml)
}

// formatAllTubeStats returns a YAML sequence of mappings, one per tube,
// each with the same fields as formatTubeStats - ordered the same way
// list-tubes orders tubes (default first, if present, then the rest
// alphabetically) so output is deterministic and easy to correlate with
// a list-tubes reply.
//
// Caller must hold s.mu.
func (s *Server) formatAllTubeStats() string {
	names := make([]string, 0, len(s.tubes))
	for name := range s.tubes {
		if name != "default" {
			names = append(names, name)
		}
	}
	sort.Strings(names)

	ordered := make([]string, 0, len(s.tubes))
	if _, ok := s.tubes["default"]; ok {
		ordered = append(ordered, "default")
	}
	ordered = append(ordered, names...)

	var b strings.Builder
	b.WriteString("---\n")
	for _, name := range ordered {
		t := s.tubes[name]
		pauseTimeLeft := int64(0)
		if t.Pause > 0 {
			left := time.Until(t.UnpauseAt)
			if left > 0 {
				pauseTimeLeft = int64(left.Seconds())
			}
		}
		fmt.Fprintf(&b, `- name: %s
  current-jobs-urgent: %d
  current-jobs-ready: %d
  current-jobs-reserved: %d
  current-jobs-delayed: %d
  current-jobs-buried: %d
  total-jobs: %d
  current-using: %d
  current-waiting: %d
  current-watching: %d
  pause: %d
  cmd-delete: %d
  cmd-pause-tube: %d
  pause-time-left: %d
  dlq-max-attempts: %d
  dlq-tube: %s
`,
			t.Name,
			t.Stat.UrgentCt,
			t.Stat.ReadyCt,
			t.Stat.ReservedCt,
			t.Stat.DelayedCt,
			t.Stat.BuriedCt,
			t.Stat.TotalJobsCt,
			t.Stat.UsingCt,
			t.Stat.WaitingCt,
			t.Stat.WatchingCt,
			int64(t.Pause.Seconds()),
			t.Stat.DeleteCt,
			t.Stat.PauseTubeCt,
			pauseTimeLeft,
			t.MaxAttempts,
			t.DeadLetterTube,
		)
	}
	return b.String()
}
