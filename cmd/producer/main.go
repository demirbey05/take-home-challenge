package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/demirbey05/take-home/internal/config"
	"github.com/demirbey05/take-home/internal/persistence"
	"github.com/demirbey05/take-home/internal/producer"
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

	cfg, err := config.LoadDefaults()
	if err != nil {
		fmt.Printf("Error loading configuration: %v\n", err)
		os.Exit(1)
	}

	var logger *slog.Logger
	var handler slog.Handler
	logLevel := slog.LevelInfo
	if cfg.LogLevel == "debug" {
		logLevel = slog.LevelDebug
	} else if cfg.LogLevel == "warn" {
		logLevel = slog.LevelWarn
	} else if cfg.LogLevel == "error" {
		logLevel = slog.LevelError
	}

	opts := &slog.HandlerOptions{Level: logLevel}
	if cfg.LogFormat == "json" {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}
	logger = slog.New(handler)
	slog.SetDefault(logger)

	logger.Info("Starting Task Producer Service...", "version", version, "buildTime", buildTime)

	if cfg.ProfilingPort > 0 {
		logger.Info("Starting profiling server", "port", cfg.ProfilingPort)
		producer.StartProfilingServer(cfg.ProfilingPort)
	}

	db, err := persistence.NewDB(cfg.DatabaseURL)
	if err != nil {
		logger.Error("Failed to initialize database", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	querier := persistence.New(db)
	repo := producer.NewRepository(querier)
	consRepo := producer.NewConsumerRepository(cfg.ConsumerURL)
	svc := producer.NewService(cfg, repo, consRepo, logger)
	controller := producer.NewController()

	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.PrometheusPort),
		Handler: controller,
	}

	go func() {
		logger.Info("Starting HTTP server", "port", cfg.PrometheusPort)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server failed", "error", err)
			os.Exit(1)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := svc.Start(ctx); err != nil {
			logger.Error("Service error", "error", err)
			cancel()
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down gracefully...")
	cancel()
	
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown error", "error", err)
	}

	logger.Info("Stopped successfully")
}
