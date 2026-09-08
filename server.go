package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	safetyMargin      = time.Second
	defaultMaxJobSize = 1 << 16 // 65536
	maxLineLen        = 224
	maxTubeName       = 200
	version           = "beanstalkd-pi-1.0.0"
)

type GlobalStats struct {
	CmdPut                uint64
	CmdPeek               uint64
	CmdPeekReady          uint64
	CmdPeekDelayed        uint64
	CmdPeekBuried         uint64
	CmdReserve            uint64
	CmdReserveWithTimeout uint64
	CmdTouch              uint64
	CmdUse                uint64
	CmdWatch              uint64
	CmdIgnore             uint64
	CmdDelete             uint64
	CmdRelease            uint64
	CmdBury               uint64
	CmdKick               uint64
	CmdStats              uint64
	CmdStatsJob           uint64
	CmdStatsTube          uint64
	CmdListTubes          uint64
	CmdListTubeUsed       uint64
	CmdListTubesWatched   uint64
	CmdPauseTube          uint64
	CmdReserveJob         uint64
	CmdKickJob            uint64
	JobTimeouts           uint64
	TotalJobs             uint64
	TotalConnections      uint64
}

type Server struct {
	listener   net.Listener
	tubes      map[string]*Tube
	conns      map[uint64]*Conn
	jobIdx     *JobIndex
	nextID     atomic.Uint64
	connID     atomic.Uint64
	persist    Persistence
	maxJobSize int

	mu sync.Mutex

	globalStats    GlobalStats
	readyCt        int64
	globalUrgentCt int64
	reservedCt     int64
	delayedCt      int64
	buriedCt       int64
	currentConns   int64
	producers      int64
	workers        int64
	waiting        int64

	drainMode  atomic.Bool
	startTime  time.Time
	instanceID string
	hostname   string
	osName     string
	platform   string

	// slowLogThreshold is the duration above which lock waits, lock
	// holds, and reserve calls are logged as warnings. Defaults to
	// defaultSlowLogThreshold; overridable via the -slow-log-threshold
	// flag.
	slowLogThreshold time.Duration

	// adminServer is the observability HTTP server (/metrics,
	// /healthz, /debug/pprof), if one was started. Guarded by mu.
	adminServer *http.Server

	closeCh chan struct{}
	wg      sync.WaitGroup
}

const defaultSlowLogThreshold = 50 * time.Millisecond

// NewServer creates a Server listening on addr, using persist as its
// persistence backend. If persist is nil, persistence is a no-op.
func NewServer(addr string, persist Persistence) (*Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	idBytes := make([]byte, 8)
	rand.Read(idBytes)

	hostname, _ := os.Hostname()

	if persist == nil {
		persist = &NoopPersistence{}
	}

	s := &Server{
		listener:         ln,
		tubes:            make(map[string]*Tube),
		conns:            make(map[uint64]*Conn),
		jobIdx:           NewJobIndex(),
		persist:          persist,
		maxJobSize:       defaultMaxJobSize,
		startTime:        time.Now(),
		instanceID:       hex.EncodeToString(idBytes),
		hostname:         hostname,
		osName:           runtime.GOOS,
		platform:         runtime.GOARCH,
		slowLogThreshold: defaultSlowLogThreshold,
		closeCh:          make(chan struct{}),
	}

	s.nextID.Store(1)
	s.makeTube("default")

	if err := s.persist.Init(); err != nil {
		ln.Close()
		return nil, fmt.Errorf("persistence init: %w", err)
	}

	jobs, err := s.persist.LoadAllJobs()
	if err != nil {
		ln.Close()
		return nil, fmt.Errorf("persistence load: %w", err)
	}
	for _, pj := range jobs {
		j := fromPersistedJob(pj)
		t := s.makeTube(j.Tube.Name)
		j.Tube = t
		s.jobIdx.Add(j)
		if j.ID >= s.nextID.Load() {
			s.nextID.Store(j.ID + 1)
		}
		s.enqueueJob(j)
	}

	return s, nil
}

func (s *Server) persistJob(j *Job) {
	if err := s.persist.StoreJob(ToPersistedJob(j)); err != nil {
		logger.Error("persist store error", "job", j.ID, "err", err)
	}
}

func (s *Server) persistDelete(id uint64) {
	if err := s.persist.DeleteJob(id); err != nil {
		logger.Error("persist delete error", "job", id, "err", err)
	}
}

// lockTraced acquires s.mu, logging a warning if the wait or the
// resulting critical section exceeds s.slowLogThreshold. op names the
// caller for the log line (e.g. "reserve", "tick"). The returned func
// must be deferred to release the lock.
func (s *Server) lockTraced(op string) func() {
	waitStart := time.Now()
	s.mu.Lock()
	if waited := time.Since(waitStart); waited > s.slowLogThreshold {
		logger.Warn("lock contention", "op", op, "waited", waited)
	}

	held := time.Now()
	return func() {
		s.mu.Unlock()
		if d := time.Since(held); d > s.slowLogThreshold {
			logger.Warn("slow critical section", "op", op, "held", d)
		}
	}
}

func (s *Server) makeTube(name string) *Tube {
	if t, ok := s.tubes[name]; ok {
		return t
	}
	t := NewTube(name)
	s.tubes[name] = t
	return t
}

// gcTube removes t from the tube map once it is both empty (no ready,
// delayed, reserved, or buried jobs) and unreferenced (no connection uses
// or watches it), mirroring stock beanstalkd's per-tube refcounting
// (tube_iref/tube_dref in tube.c) and protocol.txt: "If a tube is empty
// ... and no client refers to it, it will be deleted." The "default" tube
// is exempt: stock beanstalkd holds a permanent reference to it via the
// process-lifetime default_tube global, so it never gets collected.
//
// Callers must hold s.mu.
func (s *Server) gcTube(t *Tube) {
	if t == nil || t.Name == "default" {
		return
	}
	if t.Ready.Len() != 0 || t.Delay.Len() != 0 || t.Stat.ReservedCt != 0 || t.Stat.BuriedCt != 0 {
		return
	}
	if t.Stat.UsingCt != 0 || t.Stat.WatchingCt != 0 {
		return
	}
	delete(s.tubes, t.Name)
}

func (s *Server) Run() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGUSR1, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		for sig := range sigCh {
			switch sig {
			case syscall.SIGUSR1:
				s.drainMode.Store(true)
				logger.Info("drain mode activated")
			case syscall.SIGINT, syscall.SIGTERM:
				logger.Info("shutting down...")
				close(s.closeCh)
				s.listener.Close()
				s.shutdownAdmin()
				if err := s.persist.Close(); err != nil {
					logger.Error("persistence close error", "err", err)
				}
				return
			}
		}
	}()

	go s.tickLoop()

	logger.Info("listening", "addr", s.listener.Addr().String())

	for {
		c, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.closeCh:
				return
			default:
				logger.Error("accept error", "err", err)
				continue
			}
		}
		s.wg.Add(1)
		go s.handleConn(c)
	}
}

func (s *Server) handleConn(nc net.Conn) {
	defer s.wg.Done()

	id := s.connID.Add(1)
	c := NewConn(nc, s, id)

	s.mu.Lock()
	defaultTube := s.tubes["default"]
	c.UseTube = defaultTube
	defaultTube.Stat.UsingCt++
	c.WatchMap[defaultTube] = true
	defaultTube.Stat.WatchingCt++
	s.conns[id] = c
	s.currentConns++
	s.globalStats.TotalConnections++
	s.mu.Unlock()

	defer func() {
		nc.Close()
		s.mu.Lock()
		c.reenqueueReservedJobs()
		if c.UseTube != nil {
			c.UseTube.Stat.UsingCt--
			s.gcTube(c.UseTube)
		}
		for t := range c.WatchMap {
			t.Stat.WatchingCt--
			s.gcTube(t)
		}
		delete(s.conns, id)
		s.currentConns--
		if c.isProducer {
			s.producers--
		}
		if c.isWorker {
			s.workers--
		}
		if c.isWaiting {
			s.waiting--
			for t := range c.WatchMap {
				t.Stat.WaitingCt--
			}
		}
		s.mu.Unlock()
	}()

	c.serve()
}

func (s *Server) tickLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-s.closeCh:
			return
		case now := <-ticker.C:
			s.tick(now)
		}
	}
}

func (s *Server) tick(now time.Time) {
	unlock := s.lockTraced("tick")
	defer unlock()

	for _, t := range s.tubes {
		for t.Delay.Len() > 0 {
			j := t.Delay.Peek()
			if j.DeadlineAt.After(now) {
				break
			}
			t.Delay.Pop()
			j.State = StateReady
			j.DeadlineAt = time.Time{}
			t.Ready.Push(j)
			t.Stat.ReadyCt++
			t.Stat.DelayedCt--
			s.readyCt++
			s.delayedCt--
			if j.Pri < 1024 {
				t.Stat.UrgentCt++
				s.globalUrgentCt++
			}
			s.persistJob(j)
		}

		if t.Pause != 0 && !now.Before(t.UnpauseAt) {
			t.Pause = 0
		}
	}

	for _, c := range s.conns {
		for j := c.resHead.resNext; j != &c.resHead; {
			next := j.resNext
			if !j.DeadlineAt.IsZero() && now.After(j.DeadlineAt) {
				c.unreserveJob(j)
				j.TimeoutCt++
				s.globalStats.JobTimeouts++
				j.State = StateReady
				j.DeadlineAt = time.Time{}
				t := j.Tube
				if t != nil {
					t.Ready.Push(j)
					t.Stat.ReadyCt++
					t.Stat.ReservedCt--
					s.readyCt++
					s.reservedCt--
					if j.Pri < 1024 {
						t.Stat.UrgentCt++
						s.globalUrgentCt++
					}
				}
			}
			j = next
		}
	}

	for _, c := range s.conns {
		if !c.isWaiting {
			continue
		}

		if c.hasDeadlineSoon(now) && !c.hasReadyJobLocked() {
			c.isWaiting = false
			s.waiting--
			for t := range c.WatchMap {
				t.Stat.WaitingCt--
			}
			c.state = StateWantCommand
			go c.replyWord("DEADLINE_SOON\r\n")
			continue
		}

		if c.hasTimeout && time.Now().After(c.timeoutAt) {
			c.isWaiting = false
			s.waiting--
			for t := range c.WatchMap {
				t.Stat.WaitingCt--
			}
			c.hasTimeout = false
			c.state = StateWantCommand
			go c.replyWord("TIMED_OUT\r\n")
			continue
		}

		if j := s.findJobForConn(c); j != nil {
			c.isWaiting = false
			s.waiting--
			for t := range c.WatchMap {
				t.Stat.WaitingCt--
			}
			go c.sendReservedJob(j)
		}
	}
}

func (s *Server) findJobForConn(c *Conn) *Job {
	var best *Job
	var bestTube *Tube
	for t := range c.WatchMap {
		if t.isPaused(time.Now()) {
			continue
		}
		if t.Ready.Len() == 0 {
			continue
		}
		top := t.Ready.Peek()
		if best == nil || jobPriLess(top, best) {
			best = top
			bestTube = t
		}
	}
	if best != nil && bestTube != nil {
		bestTube.Ready.Pop()
		bestTube.Stat.ReadyCt--
		s.readyCt--
		if best.Pri < 1024 {
			bestTube.Stat.UrgentCt--
			s.globalUrgentCt--
		}
		bestTube.Stat.ReservedCt++
		s.reservedCt++
		c.reserveJob(best)
		return best
	}
	return nil
}

func (s *Server) deleteJob(j *Job) bool {
	switch j.State {
	case StateReady:
		t := j.Tube
		if t != nil {
			t.Ready.Remove(j.readyIdx)
			t.Stat.ReadyCt--
			s.readyCt--
			if j.Pri < 1024 {
				t.Stat.UrgentCt--
				s.globalUrgentCt--
			}
		}
	case StateDelayed:
		t := j.Tube
		if t != nil {
			t.Delay.Remove(j.delayIdx)
			t.Stat.DelayedCt--
			s.delayedCt--
		}
	case StateReserved:
		if j.Reserver != nil {
			j.Reserver.unreserveJob(j)
		}
		t := j.Tube
		if t != nil {
			t.Stat.ReservedCt--
			s.reservedCt--
		}
	case StateBuried:
		t := j.Tube
		if t != nil {
			t.buryRemove(j)
			t.Stat.BuriedCt--
			s.buriedCt--
		}
	default:
		return false
	}

	if j.Tube != nil {
		j.Tube.Stat.DeleteCt++
	}
	s.jobIdx.Remove(j.ID)
	s.persistDelete(j.ID)
	j.State = StateInvalid
	return true
}

func (s *Server) enqueueJob(j *Job) {
	t := j.Tube
	if j.Delay > 0 {
		j.State = StateDelayed
		j.DeadlineAt = time.Now().Add(j.Delay)
		t.Delay.Push(j)
		t.Stat.DelayedCt++
		s.delayedCt++
	} else {
		j.State = StateReady
		j.DeadlineAt = time.Time{}
		t.Ready.Push(j)
		t.Stat.ReadyCt++
		s.readyCt++
		if j.Pri < 1024 {
			t.Stat.UrgentCt++
			s.globalUrgentCt++
		}
	}
}

func (s *Server) formatStats() string {
	now := time.Now()
	uptime := int64(now.Sub(s.startTime).Seconds())

	var ru syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	utime := fmt.Sprintf("%d.%06d", ru.Utime.Sec, ru.Utime.Usec)
	stime := fmt.Sprintf("%d.%06d", ru.Stime.Sec, ru.Stime.Usec)

	gs := s.globalStats

	return fmt.Sprintf(`---
current-jobs-urgent: %d
current-jobs-ready: %d
current-jobs-reserved: %d
current-jobs-delayed: %d
current-jobs-buried: %d
cmd-put: %d
cmd-peek: %d
cmd-peek-ready: %d
cmd-peek-delayed: %d
cmd-peek-buried: %d
cmd-reserve: %d
cmd-reserve-with-timeout: %d
cmd-touch: %d
cmd-use: %d
cmd-watch: %d
cmd-ignore: %d
cmd-delete: %d
cmd-release: %d
cmd-bury: %d
cmd-kick: %d
cmd-stats: %d
cmd-stats-job: %d
cmd-stats-tube: %d
cmd-list-tubes: %d
cmd-list-tube-used: %d
cmd-list-tubes-watched: %d
cmd-pause-tube: %d
job-timeouts: %d
total-jobs: %d
max-job-size: %d
current-tubes: %d
current-connections: %d
current-producers: %d
current-workers: %d
current-waiting: %d
total-connections: %d
pid: %d
version: %s
rusage-utime: %s
rusage-stime: %s
uptime: %d
binlog-oldest-index: 0
binlog-current-index: 0
binlog-max-size: 0
binlog-records-written: 0
binlog-records-migrated: 0
draining: %s
id: %s
hostname: %s
os: %s
platform: %s
`,
		s.globalUrgentCt,
		s.readyCt,
		s.reservedCt,
		s.delayedCt,
		s.buriedCt,
		gs.CmdPut,
		gs.CmdPeek,
		gs.CmdPeekReady,
		gs.CmdPeekDelayed,
		gs.CmdPeekBuried,
		gs.CmdReserve,
		gs.CmdReserveWithTimeout,
		gs.CmdTouch,
		gs.CmdUse,
		gs.CmdWatch,
		gs.CmdIgnore,
		gs.CmdDelete,
		gs.CmdRelease,
		gs.CmdBury,
		gs.CmdKick,
		gs.CmdStats,
		gs.CmdStatsJob,
		gs.CmdStatsTube,
		gs.CmdListTubes,
		gs.CmdListTubeUsed,
		gs.CmdListTubesWatched,
		gs.CmdPauseTube,
		gs.JobTimeouts,
		gs.TotalJobs,
		s.maxJobSize,
		len(s.tubes),
		s.currentConns,
		s.producers,
		s.workers,
		s.waiting,
		gs.TotalConnections,
		os.Getpid(),
		version,
		utime,
		stime,
		uptime,
		boolStr(s.drainMode.Load()),
		s.instanceID,
		s.hostname,
		s.osName,
		s.platform,
	)
}

func (s *Server) formatTubeStats(t *Tube) string {
	pauseTimeLeft := int64(0)
	if t.Pause > 0 {
		left := time.Until(t.UnpauseAt)
		if left > 0 {
			pauseTimeLeft = int64(left.Seconds())
		}
	}

	return fmt.Sprintf(`---
name: %s
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
	)
}

func (s *Server) formatJobStats(j *Job) string {
	timeLeft := int64(0)
	if j.State == StateReserved || j.State == StateDelayed {
		left := time.Until(j.DeadlineAt)
		if left > 0 {
			timeLeft = int64(left.Seconds())
		}
	}

	tubeName := ""
	if j.Tube != nil {
		tubeName = j.Tube.Name
	}

	return fmt.Sprintf(`---
id: %d
tube: %s
state: %s
pri: %d
age: %d
delay: %d
ttr: %d
time-left: %d
file: 0
reserves: %d
timeouts: %d
releases: %d
buries: %d
kicks: %d
`,
		j.ID,
		tubeName,
		stateName(j.State),
		j.Pri,
		int64(j.age().Seconds()),
		int64(j.Delay.Seconds()),
		int64(j.TTR.Seconds()),
		timeLeft,
		j.ReserveCt,
		j.TimeoutCt,
		j.ReleaseCt,
		j.BuryCt,
		j.KickCt,
	)
}

func (s *Server) formatListTubes() string {
	result := "---\n"
	for name := range s.tubes {
		result += "- " + name + "\n"
	}
	return result
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
