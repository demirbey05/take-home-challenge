package main

import (
	"fmt"
	"os"
)

// Build-time variables injected via -ldflags
var (
	version   = "dev"
	buildTime = "unknown"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "-version" {
		fmt.Printf("producer version=%s build=%s\n", version, buildTime)
		os.Exit(0)
	}

	fmt.Println("Starting Task Producer Service...")
	// TODO: Load config, init DB, start HTTP server, produce tasks
}
