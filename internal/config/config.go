package config

import (
	"embed"
	"encoding/json"
	"fmt"
)

//go:embed defaults.json
var defaultsFS embed.FS

// Config holds shared configuration for both producer and consumer.
type Config struct {
	DatabaseURL string `json:"database_url"`
	MetricsPort int    `json:"metrics_port"`

	// Producer-specific
	MaxBacklog     int `json:"max_backlog"`
	ProduceRateMs  int `json:"produce_rate_ms"`

	// Consumer-specific
	RateLimitPerSec int `json:"rate_limit_per_sec"`
	ConsumerPort    int `json:"consumer_port"`
}

// LoadDefaults reads the embedded defaults.json and returns a Config.
func LoadDefaults() (*Config, error) {
	data, err := defaultsFS.ReadFile("defaults.json")
	if err != nil {
		return nil, fmt.Errorf("reading embedded defaults: %w", err)
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing defaults: %w", err)
	}

	return &cfg, nil
}
