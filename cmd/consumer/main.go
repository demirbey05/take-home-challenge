package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/demirbey05/take-home/internal/config"
	"github.com/demirbey05/take-home/internal/consumer"
	"github.com/demirbey05/take-home/internal/persistence"
)

var (
	version   = "dev"
	buildTime = "unknown"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "-version" {
		fmt.Printf("consumer version=%s build=%s\n", version, buildTime)
		os.Exit(0)
	}

	cfg, err := config.LoadDefaults("consumer")
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
	logWriter := io.Writer(os.Stdout)
	var logFile *os.File
	if cfg.LogFile != "" {
		if err := os.MkdirAll(filepath.Dir(cfg.LogFile), 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "Error creating log directory: %v\n", err)
		} else if f, err := os.OpenFile(cfg.LogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "Error opening log file: %v\n", err)
		} else {
			logFile = f
			logWriter = io.MultiWriter(os.Stdout, logFile)
			defer logFile.Close()
		}
	}
	if cfg.LogFormat == "json" {
		handler = slog.NewJSONHandler(logWriter, opts)
	} else {
		handler = slog.NewTextHandler(logWriter, opts)
	}
	logger = slog.New(handler)
	slog.SetDefault(logger)

	logger.Info("Starting Task Consumer Service...", "version", version, "buildTime", buildTime)

	if cfg.ProfilingPort > 0 {
		logger.Info("Starting profiling server", "port", cfg.ProfilingPort)
		consumer.StartProfilingServer(cfg.ProfilingPort)
	}

	db, err := persistence.NewDB(cfg.DatabaseURL)
	if err != nil {
		logger.Error("Failed to initialize database", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	querier := persistence.New(db)
	repo := consumer.NewRepository(querier)
	svc := consumer.NewService(cfg, repo, logger)

	metricsMux := consumer.NewMetricsMux()
	metricsServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.PrometheusPort),
		Handler: metricsMux,
	}

	restController := consumer.NewRESTController(svc)
	restServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.ConsumerPort),
		Handler: restController,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		logger.Info("Starting metrics server", "port", cfg.PrometheusPort)
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("Metrics server failed", "error", err)
		}
	}()

	go func() {
		logger.Info("Starting REST server", "port", cfg.ConsumerPort)
		if err := restServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("REST server failed", "error", err)
		}
	}()

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

	_ = metricsServer.Shutdown(shutdownCtx)
	_ = restServer.Shutdown(shutdownCtx)

	logger.Info("Stopped successfully")
}
