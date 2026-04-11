package main

import (
	"embed"
	"flag"
	"io/fs"
	"log"
	"net/http"
	"os"
	"time"

	"djs/internal/learnsite"
)

const defaultListen = "127.0.0.1:17888"

//go:embed static/*
var staticFiles embed.FS

func main() {
	var listen string
	var configPath string

	flag.StringVar(&listen, "listen", defaultListen, "learning site listen address")
	flag.StringVar(&configPath, "config", "configs/local.yaml", "learning site config path")
	flag.Parse()

	root, err := fs.Sub(staticFiles, "static")
	if err != nil {
		log.Fatalf("open embedded static files: %v", err)
	}

	logger := log.New(os.Stdout, "[learn-site] ", log.LstdFlags)
	app, err := learnsite.NewApp(root, configPath, logger)
	if err != nil {
		logger.Fatalf("initialize learning site: %v", err)
	}
	defer func() {
		if err := app.Close(); err != nil {
			logger.Printf("close learning site: %v", err)
		}
	}()

	server := &http.Server{
		Addr:              listen,
		Handler:           app.Handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	logger.Printf("serving DJS learning site at http://%s", listen)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Fatalf("start learning site: %v", err)
	}
}
