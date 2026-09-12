# beanstalkd-bench

A load-generation and latency-measurement tool for a beanstalkd server,
built on the [`beanstalkd/go-beanstalk`](https://github.com/beanstalkd/go-beanstalk)
client library. It drives concurrent producer and consumer connections
against one or more tubes and reports throughput and latency percentiles
for put, delete, and end-to-end (put-to-reserve) timings.

It speaks the stock beanstalkd wire protocol, so it works against
`beanstalkd-pi` as well as upstream beanstalkd.

## Build

```sh
go build -o beanstalkd-bench ./cmd/beanstalkd-bench
```

## Quick start

```sh
./beanstalkd-bench -addr 127.0.0.1:11300 -mode both -n 10000
```

This puts 10,000 jobs into the `bench` tube from one producer connection
while one consumer connection reserves and deletes them, then prints a
report of throughput and latency once both sides finish.

## Modes (`-mode`)

- **`both`** (default) — run producers and consumers concurrently: puts
  and reserve+deletes overlap, as in a live queue.
- **`put`** — producers only. Useful for measuring pure enqueue
  throughput/latency without any consumer contending for the CPU or lock.
- **`reserve`** — consumers only, draining whatever is already on the
  tube(s) (e.g. jobs left over from a prior `-mode put` run).
- **`idle`** — no producers or consumers at all; just opens `-idle-conns`
  connections and holds them open (connected, watching only `default`,
  never reserving) until `-duration` elapses or Ctrl-C. Use this to
  isolate the server's per-connection overhead (goroutine + bookkeeping)
  from any job traffic.

`-idle-conns` can also be layered on top of `both`/`put`/`reserve` to see
how job throughput degrades as the number of concurrently-held idle
connections grows — a proxy for many parked clients (long-poll reserves,
mostly-idle workers) sharing the server with active ones.

## Flags

| Flag | Default | Meaning |
|------|---------|---------|
| `-addr` | `127.0.0.1:11300` | beanstalkd address to connect to. |
| `-tube` | `bench` | Tube(s) to put into / reserve from. A comma-separated list (e.g. `"a,b,c"`) benchmarks multiple tubes at once: puts are round-robined across them, and consumers watch all of them. With more than one tube, `-producers`/`-consumers` default to one per tube instead of 1, so the run actually exercises concurrent connections rather than round-robining a single one. |
| `-n` | `10000` | Total jobs to put (`both`/`put` modes), or max jobs to reserve+delete before stopping (`reserve` mode). Ignored if `-duration` is set. |
| `-duration` | `0` (disabled) | Run for this long instead of a fixed job count, e.g. `30s`. |
| `-producers` | `1` | Concurrent producer connections. |
| `-consumers` | `1` | Concurrent consumer connections. |
| `-body-size` | `1024` | Job body size in bytes (minimum 8 — the first 8 bytes carry a put timestamp used for end-to-end latency). |
| `-priority` | `1024` | Job priority (0 is most urgent). |
| `-ttr` | `60s` | Job time-to-run. |
| `-delay` | `0` | Delay before a put job becomes ready. |
| `-mode` | `both` | `both`, `put`, `reserve`, or `idle` — see [Modes](#modes--mode) above. |
| `-reserve-timeout` | `1s` | Per-`RESERVE` timeout used by consumers while polling for jobs; also bounds how long a consumer can take to notice shutdown while idle. |
| `-drain-timeout` | `30s` | Max time consumers keep reserving to reach `-n` jobs: after producers finish (`both` mode), or from the start (`reserve` mode). |
| `-idle-conns` | `0` | Additional connections to open and hold idle for the run's duration — see [Modes](#modes--mode). With `-mode idle` these are the only connections opened. |

## How it measures latency

Each producer generates a body whose first 8 bytes are the put call's
start time (nanoseconds since epoch, big-endian). Each consumer, on
reserving a job, subtracts that timestamp from the current time to get
end-to-end (put → reserve) latency. Put and delete latencies are timed
directly around the respective client calls. All three are reported as
min/avg/p50/p90/p99/max over the full run.

Producer/consumer clocks must therefore be comparable — run
`beanstalkd-bench` on the same host as the server, or on hosts with
synchronized clocks, if you care about the end-to-end numbers.

## Stopping

- Fixed job count (`-duration` not set): a `both` run stops once
  producers finish and consumers either catch up to `-n` deletes or
  `-drain-timeout` elapses; a `put`-only run stops as soon as producers
  finish; a `reserve`-only run stops via its own reserve/drain logic.
- `-duration`: the run stops when the timer expires, regardless of mode.
- Ctrl-C stops any run early and still prints a report for whatever was
  measured so far.

## Example: multi-tube run with idle connections

```sh
./beanstalkd-bench -addr 127.0.0.1:11300 \
  -tube a,b,c -duration 30s -idle-conns 200 \
  -body-size 4096 -ttr 30s
```

Runs for 30 seconds against three tubes (3 producers, 3 consumers by
default), 4KB job bodies, while also holding 200 idle connections open —
useful for seeing how per-connection overhead affects throughput/latency
under a large pool of otherwise-idle clients.

## Sample output

```
beanstalkd-bench: 127.0.0.1:11300 tubes=bench mode=both producers=1 consumers=1 body=1024B elapsed=1.842s

PUT: 10000 ok, 0 errors, 5429.9 jobs/sec
PUT latency (n=10000): min=45µs avg=178µs p50=162µs p90=241µs p99=412µs max=1.203ms

DELETE: 10000 ok, 0 errors, 5429.9 jobs/sec
DELETE latency (n=10000): min=38µs avg=155µs p50=141µs p90=219µs p99=387µs max=980µs

END-TO-END (put → reserve) (n=10000): min=61µs avg=203µs p50=185µs p90=278µs p99=460µs max=1.4ms
```

(Numbers above are illustrative, not a benchmark result.)
