// storaged is the object storage daemon for GitHub Actions self-hosted
// runners running on a persistent disk with replication and a Cloudflare
// tunnel fronting a custom REST API.
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"storaged/internal/boot"
)

func main() {
	cfgPath := flag.String("config", "", "path to TOML config (default: env-only)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := boot.Run(ctx, *cfgPath); err != nil {
		println("storaged: " + err.Error())
		os.Exit(1)
	}
}