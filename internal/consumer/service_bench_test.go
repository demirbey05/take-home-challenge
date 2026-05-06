package consumer

import (
	"context"
	"io"
	"log/slog"
	"math"
	"runtime/debug"
	"testing"

	"github.com/demirbey05/take-home/internal/config"
	"github.com/demirbey05/take-home/internal/persistence"
	"github.com/stretchr/testify/mock"
	"golang.org/x/time/rate"
)

func BenchmarkHandleTask_GCTuning(b *testing.B) {
	scenarios := []struct {
		name       string
		gogc       int
		gomemlimit int64
	}{
		{"Baseline_GOGC100", 100, math.MaxInt64},
		{"Aggressive_GOGC50", 50, math.MaxInt64},
		{"Lazy_GOGC200", 200, math.MaxInt64},
		{"VeryLazy_GOGC500", 500, math.MaxInt64},
		{"Limit_64MB_GOGC100", 100, 64 * 1024 * 1024},
		{"Limit_32MB_GOGC100", 100, 32 * 1024 * 1024},
		{"Limit_16MB_GOGC50", 50, 16 * 1024 * 1024},
	}

	for _, sc := range scenarios {
		b.Run(sc.name, func(b *testing.B) {
			// Apply GC settings
			oldGOGC := debug.SetGCPercent(sc.gogc)
			oldLimit := debug.SetMemoryLimit(sc.gomemlimit)
			defer func() {
				debug.SetGCPercent(oldGOGC)
				debug.SetMemoryLimit(oldLimit)
			}()

			cfg := &config.Config{RateLimitPerSec: 1000000}
			repo := new(mockRepository)
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))

			s := &service{
				cfg:     cfg,
				repo:    repo,
				logger:  logger,
				limiter: rate.NewLimiter(rate.Inf, 0), // No rate limiting to max out CPU
			}

			// Value=0 means zero sleep time in consumer, maximizing CPU/Alloc pressure
			task := persistence.Task{ID: 1, Type: 3, Value: 0, State: "received"}

			// Mock db queries
			repo.On("UpdateTaskState", mock.Anything, task.ID, "processing").Return(nil)
			repo.On("UpdateTaskState", mock.Anything, task.ID, "done").Return(nil)
			repo.On("SumProcessedValuesByType", mock.Anything, int(task.Type)).Return(int64(42), nil)

			ctx := context.Background()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = s.HandleTask(ctx, task)
			}
			b.StopTimer()
		})
	}
}
