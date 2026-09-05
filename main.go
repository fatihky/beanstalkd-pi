package main

import (
	"flag"
	"log"
)

func main() {
	addr := flag.String("addr", ":11300", "listen address")
	flag.Parse()

	srv, err := NewServer(*addr)
	if err != nil {
		log.Fatalf("failed to start server: %v", err)
	}

	srv.Run()
}
