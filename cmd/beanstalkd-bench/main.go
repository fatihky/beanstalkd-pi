// Command beanstalkd-bench is a load-generation and latency-measurement
// tool for a beanstalkd server, built on the beanstalkd/go-beanstalk
// client library. It drives concurrent producer and consumer connections
// against a tube and reports throughput and latency percentiles for put,
// delete, and end-to-end (put-to-reserve) timings.
package main

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	beanstalk "github.com/beanstalkd/go-beanstalk"
)

func main() {
	log.SetFlags(0)
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "beanstalkd-bench:", err)
		os.Exit(2)
	}
	if err := run(cfg); err != nil {
		fmt.Fprintln(os.Stderr, "beanstalkd-bench:", err)
		os.Exit(1)
	}
}

type config struct {
	addr           string
	tubes          []string
	numJobs        int
	duration       time.Duration
	producers      int
	consumers      int
	bodySize       int
	priority       uint
	ttr            time.Duration
	delay          time.Duration
	mode           string
	reserveTimeout time.Duration
	drainTimeout   time.Duration
}

func parseFlags(args []string) (*config, error) {
	fs := flag.NewFlagSet("beanstalkd-bench", flag.ContinueOnError)
	cfg := &config{}
	var tubeFlag string
	fs.StringVar(&cfg.addr, "addr", "127.0.0.1:11300", "beanstalkd address")
	fs.StringVar(&tubeFlag, "tube", "bench", "tube(s) to put jobs into / reserve from; a comma-separated list (e.g. \"a,b,c\") benchmarks multiple tubes at once, with puts round-robined across them and consumers watching all of them")
	fs.IntVar(&cfg.numJobs, "n", 10000, "total jobs to put (put/both modes), or max jobs to reserve+delete before stopping (reserve mode); ignored if -duration is set")
	fs.DurationVar(&cfg.duration, "duration", 0, "run for this long instead of a fixed job count, e.g. 30s")
	fs.IntVar(&cfg.producers, "producers", 1, "number of concurrent producer connections (default: len(tubes) when multiple tubes are given and -producers is not set)")
	fs.IntVar(&cfg.consumers, "consumers", 1, "number of concurrent consumer connections (default: len(tubes) when multiple tubes are given and -consumers is not set)")
	fs.IntVar(&cfg.bodySize, "body-size", 1024, "job body size in bytes (minimum 8)")
	fs.UintVar(&cfg.priority, "priority", 1024, "job priority (0 is most urgent)")
	fs.DurationVar(&cfg.ttr, "ttr", 60*time.Second, "job time-to-run")
	fs.DurationVar(&cfg.delay, "delay", 0, "delay before a put job becomes ready")
	fs.StringVar(&cfg.mode, "mode", "both", "what to run: \"both\" (put and reserve/delete), \"put\", or \"reserve\"")
	fs.DurationVar(&cfg.reserveTimeout, "reserve-timeout", 1*time.Second, "per-RESERVE timeout used by consumers while polling for jobs; also bounds how long a consumer can take to notice shutdown while idle")
	fs.DurationVar(&cfg.drainTimeout, "drain-timeout", 30*time.Second, "max time consumers keep reserving to reach -n jobs: after producers finish (both mode), or from the start (reserve mode)")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	producersSet, consumersSet := false, false
	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "producers":
			producersSet = true
		case "consumers":
			consumersSet = true
		}
	})

	switch cfg.mode {
	case "both", "put", "reserve":
	default:
		return nil, fmt.Errorf("invalid -mode %q: must be \"both\", \"put\", or \"reserve\"", cfg.mode)
	}
	for _, t := range strings.Split(tubeFlag, ",") {
		t = strings.TrimSpace(t)
		if t != "" {
			cfg.tubes = append(cfg.tubes, t)
		}
	}
	if len(cfg.tubes) == 0 {
		return nil, errors.New("-tube must name at least one tube")
	}
	// With multiple tubes and no explicit -producers/-consumers, default to
	// one goroutine per tube so a multi-tube benchmark actually exercises
	// concurrent connections instead of round-robining a single one.
	if len(cfg.tubes) > 1 {
		if !producersSet {
			cfg.producers = len(cfg.tubes)
		}
		if !consumersSet {
			cfg.consumers = len(cfg.tubes)
		}
	}
	if cfg.bodySize < 8 {
		cfg.bodySize = 8
	}
	if cfg.mode != "reserve" && cfg.producers < 1 {
		return nil, errors.New("-producers must be >= 1")
	}
	if cfg.mode != "put" && cfg.consumers < 1 {
		return nil, errors.New("-consumers must be >= 1")
	}
	if cfg.duration <= 0 && cfg.numJobs <= 0 {
		return nil, errors.New("-n must be >= 1 when -duration is not set")
	}
	return cfg, nil
}

// stats holds the atomically-updated counters shared across all
// producer/consumer goroutines.
type stats struct {
	putCount    int64
	putErr      int64
	deleteCount int64
	deleteErr   int64
	reserveErr  int64
}

func run(cfg *config) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Ctrl-C stops the run early and still prints a report for whatever
	// was measured so far.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	go func() {
		select {
		case <-sigCh:
			cancel()
		case <-ctx.Done():
		}
	}()

	if cfg.duration > 0 {
		var timerCancel context.CancelFunc
		ctx, timerCancel = context.WithTimeout(ctx, cfg.duration)
		defer timerCancel()
	}

	st := &stats{}
	var wg sync.WaitGroup

	runProducers := cfg.mode != "reserve"
	runConsumers := cfg.mode != "put"

	putLat := make([][]time.Duration, 0)
	e2eLat := make([][]time.Duration, 0)
	delLat := make([][]time.Duration, 0)
	var latMu sync.Mutex // guards the three slices above (append only, once per goroutine)

	producersDone := make(chan struct{})
	start := time.Now()

	if runProducers {
		share, remainder := 0, 0
		if cfg.duration <= 0 {
			share = cfg.numJobs / cfg.producers
			remainder = cfg.numJobs % cfg.producers
		}
		wg.Add(cfg.producers)
		var pwg sync.WaitGroup
		pwg.Add(cfg.producers)
		for i := 0; i < cfg.producers; i++ {
			n := share
			if i < remainder {
				n++
			}
			go func(id, n int) {
				defer wg.Done()
				defer pwg.Done()
				local := runProducer(ctx, cfg, id, n, st)
				latMu.Lock()
				putLat = append(putLat, local)
				latMu.Unlock()
			}(i, n)
		}
		go func() {
			pwg.Wait()
			close(producersDone)
		}()
	} else {
		close(producersDone)
	}

	if runConsumers {
		wg.Add(cfg.consumers)
		for i := 0; i < cfg.consumers; i++ {
			go func(id int) {
				defer wg.Done()
				e2e, del := runConsumer(ctx, cfg, st)
				latMu.Lock()
				e2eLat = append(e2eLat, e2e)
				delLat = append(delLat, del)
				latMu.Unlock()
			}(i)
		}
	}

	// In a fixed-count "both" run, stop once producers are done and either
	// consumers have caught up or the drain timeout elapses. In "put" mode
	// there are no consumers to wait for, so stop as soon as producers
	// finish. "reserve" mode and any -duration run stop via ctx above.
	if cfg.duration <= 0 {
		<-producersDone
		if cfg.mode == "put" {
			cancel()
		} else {
			target := int64(cfg.numJobs)
			deadline := time.After(cfg.drainTimeout)
		drain:
			for {
				if atomic.LoadInt64(&st.deleteCount)+atomic.LoadInt64(&st.deleteErr) >= target {
					break drain
				}
				select {
				case <-deadline:
					break drain
				case <-ctx.Done():
					break drain
				case <-time.After(50 * time.Millisecond):
				}
			}
			cancel()
		}
	}

	wg.Wait()
	elapsed := time.Since(start)

	report(cfg, st, elapsed, flatten(putLat), flatten(e2eLat), flatten(delLat))
	return nil
}

// runProducer dials its own connection and puts n jobs (or, if n <= 0,
// runs until ctx is done), returning the per-job Put latencies for the
// jobs it successfully put. When cfg.tubes names more than one tube,
// this producer round-robins its puts across all of them, offset by id
// so that concurrent producers don't all start on the same tube.
func runProducer(ctx context.Context, cfg *config, id, n int, st *stats) []time.Duration {
	conn, err := beanstalk.Dial("tcp", cfg.addr)
	if err != nil {
		log.Printf("producer: dial %s: %v", cfg.addr, err)
		if n > 0 {
			atomic.AddInt64(&st.putErr, int64(n))
		}
		return nil
	}
	defer conn.Close()

	body := make([]byte, cfg.bodySize)
	for i := 8; i < len(body); i++ {
		body[i] = 'x'
	}

	var local []time.Duration
	if n > 0 {
		local = make([]time.Duration, 0, n)
	}

	numTubes := len(cfg.tubes)

loop:
	for i := 0; n <= 0 || i < n; i++ {
		select {
		case <-ctx.Done():
			break loop
		default:
		}

		conn.Tube.Name = cfg.tubes[(id+i)%numTubes]
		binary.BigEndian.PutUint64(body[:8], uint64(time.Now().UnixNano()))
		t0 := time.Now()
		_, err := conn.Put(body, uint32(cfg.priority), cfg.delay, cfg.ttr)
		elapsed := time.Since(t0)
		if err != nil {
			atomic.AddInt64(&st.putErr, 1)
			continue
		}
		atomic.AddInt64(&st.putCount, 1)
		local = append(local, elapsed)
	}
	return local
}

// runConsumer dials its own connection and reserve+deletes jobs from
// cfg.tubes until ctx is done, returning the end-to-end (put-to-reserve)
// and delete latencies it observed. It watches all of cfg.tubes, so
// with multiple tubes each consumer reserves from whichever one has a
// job ready first.
func runConsumer(ctx context.Context, cfg *config, st *stats) (e2e, del []time.Duration) {
	conn, err := beanstalk.Dial("tcp", cfg.addr)
	if err != nil {
		log.Printf("consumer: dial %s: %v", cfg.addr, err)
		return nil, nil
	}
	defer conn.Close()
	conn.TubeSet = *beanstalk.NewTubeSet(conn, cfg.tubes...)

	for {
		select {
		case <-ctx.Done():
			return e2e, del
		default:
		}

		id, body, err := conn.Reserve(cfg.reserveTimeout)
		if err != nil {
			if errors.Is(err, beanstalk.ErrTimeout) {
				continue // no job showed up within reserve-timeout; poll again
			}
			atomic.AddInt64(&st.reserveErr, 1)
			continue
		}
		now := time.Now()
		if len(body) >= 8 {
			putNanos := int64(binary.BigEndian.Uint64(body[:8]))
			e2e = append(e2e, now.Sub(time.Unix(0, putNanos)))
		}

		t0 := time.Now()
		err = conn.Delete(id)
		elapsed := time.Since(t0)
		if err != nil {
			atomic.AddInt64(&st.deleteErr, 1)
			continue
		}
		atomic.AddInt64(&st.deleteCount, 1)
		del = append(del, elapsed)
	}
}

func flatten(chunks [][]time.Duration) []time.Duration {
	n := 0
	for _, c := range chunks {
		n += len(c)
	}
	out := make([]time.Duration, 0, n)
	for _, c := range chunks {
		out = append(out, c...)
	}
	return out
}

func report(cfg *config, st *stats, elapsed time.Duration, putLat, e2eLat, delLat []time.Duration) {
	fmt.Printf("beanstalkd-bench: %s tubes=%s mode=%s producers=%d consumers=%d body=%dB elapsed=%s\n\n",
		cfg.addr, strings.Join(cfg.tubes, ","), cfg.mode, cfg.producers, cfg.consumers, cfg.bodySize, elapsed.Round(time.Millisecond))

	if cfg.mode != "reserve" {
		printSection("PUT", st.putCount, st.putErr, elapsed, putLat)
	}
	if cfg.mode != "put" {
		printSection("DELETE", st.deleteCount, st.deleteErr, elapsed, delLat)
		if len(e2eLat) > 0 {
			printLatencyOnly("END-TO-END (put → reserve)", e2eLat)
		}
		if st.reserveErr > 0 {
			fmt.Printf("reserve errors: %d\n\n", st.reserveErr)
		}
	}
}

func printSection(name string, count, errCount int64, elapsed time.Duration, lat []time.Duration) {
	throughput := float64(count) / elapsed.Seconds()
	fmt.Printf("%s: %d ok, %d errors, %.1f jobs/sec\n", name, count, errCount, throughput)
	printLatencyOnly(name+" latency", lat)
}

func printLatencyOnly(label string, lat []time.Duration) {
	if len(lat) == 0 {
		fmt.Printf("%s: n/a\n\n", label)
		return
	}
	sorted := append([]time.Duration(nil), lat...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	fmt.Printf("%s (n=%d): min=%s avg=%s p50=%s p90=%s p99=%s max=%s\n\n",
		label, len(sorted),
		sorted[0].Round(time.Microsecond),
		avg(sorted).Round(time.Microsecond),
		percentile(sorted, 50).Round(time.Microsecond),
		percentile(sorted, 90).Round(time.Microsecond),
		percentile(sorted, 99).Round(time.Microsecond),
		sorted[len(sorted)-1].Round(time.Microsecond),
	)
}

// percentile returns the p-th percentile (0-100) of a slice already
// sorted in ascending order.
func percentile(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 1 {
		return sorted[0]
	}
	idx := (p * (len(sorted) - 1)) / 100
	return sorted[idx]
}

func avg(d []time.Duration) time.Duration {
	var total time.Duration
	for _, v := range d {
		total += v
	}
	return total / time.Duration(len(d))
}
