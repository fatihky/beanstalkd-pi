package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	showVersion := flag.Bool("version", false, "print version information and exit")
	addr := flag.String("addr", ":11300", "listen address")
	dbPath := flag.String("db", "", "path to SQLite database file for persistence (empty disables persistence)")
	httpAddr := flag.String("http-addr", ":11301", "address for the observability HTTP server (/metrics, /healthz); empty disables it")
	enablePprof := flag.Bool("pprof", false, "expose net/http/pprof debug endpoints on the observability HTTP server")
	logLevel := flag.String("log-level", "info", "log level: debug, info, warn, error")
	logFormat := flag.String("log-format", "text", "log output format: text or json")
	maxJobSize := flag.Int("z", defaultMaxJobSize, "maximum job body size in bytes")
	slowLogThreshold := flag.Duration("slow-log-threshold", defaultSlowLogThreshold, "log a warning when a reserve call or a server-lock hold/wait exceeds this duration")
	flag.Parse()

	if *showVersion {
		fmt.Println(versionInfo())
		os.Exit(0)
	}

	initLogger(*logLevel, *logFormat)

	var persist Persistence
	if *dbPath != "" {
		persist = NewSQLitePersistence(*dbPath)
	}

	srv, err := NewServer(*addr, persist)
	if err != nil {
		logger.Error("failed to start server", "err", err)
		os.Exit(1)
	}
	srv.maxJobSize = *maxJobSize
	srv.slowLogThreshold = *slowLogThreshold

	if *httpAddr != "" {
		go srv.serveAdmin(*httpAddr, *enablePprof)
	}

	srv.Run()
}
