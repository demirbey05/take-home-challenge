package config

import (
	"embed"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
)

//go:embed defaults.consumer.json
//go:embed defaults.producer.json
var defaultsFS embed.FS

// Config holds shared configuration for both producer and consumer.
type Config struct {
	// Database
	DatabaseURL string `json:"database_url"`

	// Prometheus
	PrometheusPort int `json:"prometheus_port"`
	// PrometheusEndpoint is fixed at /metrics and not configurable.

	// Communication
	ConsumerURL string `json:"consumer_url"`

	// Producer-specific
	MaxBacklog        int `json:"max_backlog"`
	ProduceRatePerSec int `json:"produce_rate_per_sec"`

	// Consumer-specific
	RateLimitPerSec int `json:"rate_limit_per_sec"`
	ConsumerPort    int `json:"consumer_port"`

	// Logging
	LogLevel  string `json:"log_level"`  // debug, info, warn, error
	LogFormat string `json:"log_format"` // console, json
	LogFile   string `json:"log_file"`   // optional, logs are always written to stdout as well

	// Profiling
	ProfilingPort int `json:"profiling_port"`
}

// LoadDefaults reads the embedded defaults.json and returns a Config.
func LoadDefaults(service string) (*Config, error) {
	data, err := defaultsFS.ReadFile("defaults." + service + ".json")
	if err != nil {
		return nil, fmt.Errorf("reading embedded defaults: %w", err)
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing defaults: %w", err)
	}

	// Environment variable overrides
	applyEnvOverrides(&cfg)

	return &cfg, nil
}

// applyEnvOverrides applies environment variable overrides to the config.
// Environment variables take precedence over the embedded defaults.
func applyEnvOverrides(cfg *Config) {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		cfg.DatabaseURL = v
	}
	if v := os.Getenv("PROMETHEUS_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.PrometheusPort = n
		}
	}
	if v := os.Getenv("CONSUMER_URL"); v != "" {
		cfg.ConsumerURL = v
	}
	if v := os.Getenv("MAX_BACKLOG"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.MaxBacklog = n
		}
	}
	if v := os.Getenv("PRODUCE_RATE_PER_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.ProduceRatePerSec = n
		}
	}
	if v := os.Getenv("RATE_LIMIT_PER_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.RateLimitPerSec = n
		}
	}
	if v := os.Getenv("CONSUMER_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.ConsumerPort = n
		}
	}
	if v := os.Getenv("LOG_LEVEL"); v != "" {
		cfg.LogLevel = v
	}
	if v := os.Getenv("LOG_FORMAT"); v != "" {
		cfg.LogFormat = v
	}
	if v := os.Getenv("LOG_FILE"); v != "" {
		cfg.LogFile = v
	}
	if v := os.Getenv("PROFILING_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.ProfilingPort = n
		}
	}
}
