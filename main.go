package main

import (
	"flag"
	"log"
)

func main() {
	addr := flag.String("addr", ":11300", "listen address")
	dbPath := flag.String("db", "", "path to SQLite database file for persistence (empty disables persistence)")
	flag.Parse()

	var persist Persistence
	if *dbPath != "" {
		persist = NewSQLitePersistence(*dbPath)
	}

	srv, err := NewServer(*addr, persist)
	if err != nil {
		log.Fatalf("failed to start server: %v", err)
	}

	srv.Run()
}
