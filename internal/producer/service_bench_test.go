package producer

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
)

func BenchmarkProcessNext_GCTuning(b *testing.B) {
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

			cfg := &config.Config{MaxBacklog: 1000000} // High backlog to not hit limits
			repo := new(mockRepository)
			consRepo := new(mockConsumerRepository)
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))

			// Setup mocks to always succeed and return quickly
			repo.On("GetPendingTasksCount", mock.Anything).Return(0, nil)
			mockTask := persistence.Task{ID: 1, Type: 5, Value: 50, State: "received"}
			repo.On("CreateTask", mock.Anything, mock.Anything, mock.Anything).Return(mockTask, nil)
			consRepo.On("SendTask", mock.Anything, mockTask).Return(nil)

			s := &service{
				cfg:      cfg,
				repo:     repo,
				consRepo: consRepo,
				logger:   logger,
				sendSem:  make(chan struct{}, maxInflightSends),
			}

			ctx := context.Background()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = s.processNext(ctx)
			}
			b.StopTimer()

			// Wait for async sends to finish so we don't leak goroutines across benchmarks
			s.wg.Wait()
		})
	}
}
