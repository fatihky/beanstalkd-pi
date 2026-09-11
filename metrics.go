package main

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"time"
)

// tubeSnapshot is a point-in-time, lock-free copy of one tube's counters,
// taken while holding Server.mu.
type tubeSnapshot struct {
	name   string
	stat   TubeStats
	paused bool
}

// statsSnapshot is a point-in-time, lock-free copy of everything the
// /metrics exporter needs. Taking a snapshot under Server.mu and then
// formatting it without the lock held keeps a metrics scrape from
// blocking job processing for the duration of the (relatively slow)
// text formatting and HTTP write.
type statsSnapshot struct {
	global GlobalStats

	readyCt      int64
	urgentCt     int64
	reservedCt   int64
	delayedCt    int64
	buriedCt     int64
	currentConns int64
	producers    int64
	workers      int64
	waiting      int64

	tubes []tubeSnapshot

	draining bool
	uptime   time.Duration
}

// snapshotStats copies out the server's counters under the lock so
// formatting can happen without holding it.
func (s *Server) snapshotStats() statsSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	snap := statsSnapshot{
		global:       s.globalStats,
		readyCt:      s.readyCt,
		urgentCt:     s.globalUrgentCt,
		reservedCt:   s.reservedCt,
		delayedCt:    s.delayedCt,
		buriedCt:     s.buriedCt,
		currentConns: s.currentConns,
		producers:    s.producers,
		workers:      s.workers,
		waiting:      s.waiting,
		draining:     s.drainMode.Load(),
		uptime:       time.Since(s.startTime),
	}

	snap.tubes = make([]tubeSnapshot, 0, len(s.tubes))
	now := time.Now()
	for name, t := range s.tubes {
		snap.tubes = append(snap.tubes, tubeSnapshot{
			name:   name,
			stat:   t.Stat,
			paused: t.isPaused(now),
		})
	}
	sort.Slice(snap.tubes, func(i, j int) bool { return snap.tubes[i].name < snap.tubes[j].name })

	return snap
}

// cmdCounters lists the GlobalStats command counters, in the order they
// should be rendered.
func cmdCounters(gs GlobalStats) []struct {
	name  string
	value uint64
} {
	return []struct {
		name  string
		value uint64
	}{
		{"put", gs.CmdPut},
		{"peek", gs.CmdPeek},
		{"peek_ready", gs.CmdPeekReady},
		{"peek_delayed", gs.CmdPeekDelayed},
		{"peek_buried", gs.CmdPeekBuried},
		{"reserve", gs.CmdReserve},
		{"reserve_with_timeout", gs.CmdReserveWithTimeout},
		{"reserve_job", gs.CmdReserveJob},
		{"touch", gs.CmdTouch},
		{"use", gs.CmdUse},
		{"watch", gs.CmdWatch},
		{"ignore", gs.CmdIgnore},
		{"delete", gs.CmdDelete},
		{"release", gs.CmdRelease},
		{"bury", gs.CmdBury},
		{"kick", gs.CmdKick},
		{"kick_tube", gs.CmdKickTube},
		{"kick_job", gs.CmdKickJob},
		{"stats", gs.CmdStats},
		{"stats_job", gs.CmdStatsJob},
		{"stats_tube", gs.CmdStatsTube},
		{"list_tubes", gs.CmdListTubes},
		{"list_tube_used", gs.CmdListTubeUsed},
		{"list_tubes_watched", gs.CmdListTubesWatched},
		{"pause_tube", gs.CmdPauseTube},
		{"ping", gs.CmdPing},
	}
}

// writeMetrics renders snap in Prometheus text exposition format.
func writeMetrics(w io.Writer, snap statsSnapshot) {
	up := float64(1)
	fmt.Fprintf(w, "# HELP beanstalkd_up Whether the beanstalkd process is up.\n")
	fmt.Fprintf(w, "# TYPE beanstalkd_up gauge\n")
	fmt.Fprintf(w, "beanstalkd_up %g\n", up)

	fmt.Fprintf(w, "# HELP beanstalkd_uptime_seconds Seconds since the server started.\n")
	fmt.Fprintf(w, "# TYPE beanstalkd_uptime_seconds gauge\n")
	fmt.Fprintf(w, "beanstalkd_uptime_seconds %g\n", snap.uptime.Seconds())

	fmt.Fprintf(w, "# HELP beanstalkd_draining Whether the server is in drain mode (1) or not (0).\n")
	fmt.Fprintf(w, "# TYPE beanstalkd_draining gauge\n")
	fmt.Fprintf(w, "beanstalkd_draining %d\n", boolInt(snap.draining))

	// Global job-state gauges.
	fmt.Fprintf(w, "# HELP beanstalkd_jobs Current number of jobs in each state.\n")
	fmt.Fprintf(w, "# TYPE beanstalkd_jobs gauge\n")
	fmt.Fprintf(w, "beanstalkd_jobs{state=\"ready\"} %d\n", snap.readyCt)
	fmt.Fprintf(w, "beanstalkd_jobs{state=\"urgent\"} %d\n", snap.urgentCt)
	fmt.Fprintf(w, "beanstalkd_jobs{state=\"reserved\"} %d\n", snap.reservedCt)
	fmt.Fprintf(w, "beanstalkd_jobs{state=\"delayed\"} %d\n", snap.delayedCt)
	fmt.Fprintf(w, "beanstalkd_jobs{state=\"buried\"} %d\n", snap.buriedCt)

	fmt.Fprintf(w, "# HELP beanstalkd_jobs_total Total number of jobs ever created.\n")
	fmt.Fprintf(w, "# TYPE beanstalkd_jobs_total counter\n")
	fmt.Fprintf(w, "beanstalkd_jobs_total %d\n", snap.global.TotalJobs)

	fmt.Fprintf(w, "# HELP beanstalkd_job_timeouts_total Total number of reserved jobs that timed out (TTR expired).\n")
	fmt.Fprintf(w, "# TYPE beanstalkd_job_timeouts_total counter\n")
	fmt.Fprintf(w, "beanstalkd_job_timeouts_total %d\n", snap.global.JobTimeouts)

	// Connection gauges.
	fmt.Fprintf(w, "# HELP beanstalkd_connections Current number of connections, by role.\n")
	fmt.Fprintf(w, "# TYPE beanstalkd_connections gauge\n")
	fmt.Fprintf(w, "beanstalkd_connections{role=\"current\"} %d\n", snap.currentConns)
	fmt.Fprintf(w, "beanstalkd_connections{role=\"producers\"} %d\n", snap.producers)
	fmt.Fprintf(w, "beanstalkd_connections{role=\"workers\"} %d\n", snap.workers)
	fmt.Fprintf(w, "beanstalkd_connections{role=\"waiting\"} %d\n", snap.waiting)

	fmt.Fprintf(w, "# HELP beanstalkd_connections_total Total number of connections ever accepted.\n")
	fmt.Fprintf(w, "# TYPE beanstalkd_connections_total counter\n")
	fmt.Fprintf(w, "beanstalkd_connections_total %d\n", snap.global.TotalConnections)

	fmt.Fprintf(w, "# HELP beanstalkd_tubes_current Current number of tubes.\n")
	fmt.Fprintf(w, "# TYPE beanstalkd_tubes_current gauge\n")
	fmt.Fprintf(w, "beanstalkd_tubes_current %d\n", len(snap.tubes))

	// Command counters.
	fmt.Fprintf(w, "# HELP beanstalkd_commands_total Total number of commands processed, by command.\n")
	fmt.Fprintf(w, "# TYPE beanstalkd_commands_total counter\n")
	for _, c := range cmdCounters(snap.global) {
		fmt.Fprintf(w, "beanstalkd_commands_total{command=%q} %d\n", c.name, c.value)
	}

	// Per-tube gauges.
	fmt.Fprintf(w, "# HELP beanstalkd_tube_jobs Current number of jobs in each state, per tube.\n")
	fmt.Fprintf(w, "# TYPE beanstalkd_tube_jobs gauge\n")
	for _, t := range snap.tubes {
		fmt.Fprintf(w, "beanstalkd_tube_jobs{tube=%q,state=\"ready\"} %d\n", t.name, t.stat.ReadyCt)
		fmt.Fprintf(w, "beanstalkd_tube_jobs{tube=%q,state=\"urgent\"} %d\n", t.name, t.stat.UrgentCt)
		fmt.Fprintf(w, "beanstalkd_tube_jobs{tube=%q,state=\"reserved\"} %d\n", t.name, t.stat.ReservedCt)
		fmt.Fprintf(w, "beanstalkd_tube_jobs{tube=%q,state=\"delayed\"} %d\n", t.name, t.stat.DelayedCt)
		fmt.Fprintf(w, "beanstalkd_tube_jobs{tube=%q,state=\"buried\"} %d\n", t.name, t.stat.BuriedCt)
	}

	fmt.Fprintf(w, "# HELP beanstalkd_tube_connections Current number of connections using/watching/waiting on a tube.\n")
	fmt.Fprintf(w, "# TYPE beanstalkd_tube_connections gauge\n")
	for _, t := range snap.tubes {
		fmt.Fprintf(w, "beanstalkd_tube_connections{tube=%q,role=\"using\"} %d\n", t.name, t.stat.UsingCt)
		fmt.Fprintf(w, "beanstalkd_tube_connections{tube=%q,role=\"watching\"} %d\n", t.name, t.stat.WatchingCt)
		fmt.Fprintf(w, "beanstalkd_tube_connections{tube=%q,role=\"waiting\"} %d\n", t.name, t.stat.WaitingCt)
	}

	fmt.Fprintf(w, "# HELP beanstalkd_tube_paused Whether a tube is currently paused (1) or not (0).\n")
	fmt.Fprintf(w, "# TYPE beanstalkd_tube_paused gauge\n")
	for _, t := range snap.tubes {
		fmt.Fprintf(w, "beanstalkd_tube_paused{tube=%q} %d\n", t.name, boolInt(t.paused))
	}

	fmt.Fprintf(w, "# HELP beanstalkd_tube_jobs_total Total number of jobs ever put into a tube.\n")
	fmt.Fprintf(w, "# TYPE beanstalkd_tube_jobs_total counter\n")
	for _, t := range snap.tubes {
		fmt.Fprintf(w, "beanstalkd_tube_jobs_total{tube=%q} %d\n", t.name, t.stat.TotalJobsCt)
	}

	fmt.Fprintf(w, "# HELP beanstalkd_tube_deletes_total Total number of jobs deleted from a tube.\n")
	fmt.Fprintf(w, "# TYPE beanstalkd_tube_deletes_total counter\n")
	for _, t := range snap.tubes {
		fmt.Fprintf(w, "beanstalkd_tube_deletes_total{tube=%q} %d\n", t.name, t.stat.DeleteCt)
	}
}

// handleMetrics serves the Prometheus text exposition format.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	snap := s.snapshotStats()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	writeMetrics(w, snap)
}

// handleHealthz is a cheap liveness probe for orchestrators. It does not
// take the server lock, so it stays responsive even under heavy
// contention on the main event loop.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "ok")
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
