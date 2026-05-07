package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/demirbey05/take-home/internal/config"
	"github.com/demirbey05/take-home/internal/persistence"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/time/rate"
)

var (
	tasksProcessing = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "consumer_tasks_processing_current",
		Help: "The current number of tasks being processed",
	})
	tasksReceivedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "consumer_tasks_received_total",
		Help: "The total number of tasks received by the consumer",
	})
	tasksProcessedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "consumer_tasks_processed_total",
		Help: "The total number of tasks processed by the consumer",
	})
	tasksDoneTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "consumer_tasks_done_total",
		Help: "The total number of tasks completely processed",
	})

	tasksByTypeTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "consumer_tasks_by_type_total",
		Help: "The total number of tasks processed partitioned by type",
	}, []string{"type"})
	taskValueSumByType = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "consumer_task_value_sum_by_type_total",
		Help: "The sum of the value field for processed tasks partitioned by type",
	}, []string{"type"})
)

type Service interface {
	HandleTask(ctx context.Context, task persistence.Task) error
	Start(ctx context.Context) error
}

type service struct {
	cfg     *config.Config
	repo    Repository
	logger  *slog.Logger
	limiter *rate.Limiter
	wg      sync.WaitGroup
}

func NewService(cfg *config.Config, repo Repository, logger *slog.Logger) Service {
	// Configure rate limiter
	rateLimit := cfg.RateLimitPerSec
	if rateLimit <= 0 {
		rateLimit = 20 // fail-safe default
	}
	limit := rate.Limit(rateLimit)
	limiter := rate.NewLimiter(limit, rateLimit)

	return &service{
		cfg:     cfg,
		repo:    repo,
		logger:  logger,
		limiter: limiter,
	}
}

func (s *service) HandleTask(ctx context.Context, task persistence.Task) error {
	// Apply rate limiting
	if err := s.limiter.Wait(ctx); err != nil {
		return fmt.Errorf("rate limiter wait: %w", err)
	}

	tasksReceivedTotal.Inc()

	// 1. Set to "processing"
	if err := s.repo.UpdateTaskState(ctx, task.ID, "processing"); err != nil {
		return fmt.Errorf("updating state to processing: %w", err)
	}
	tasksProcessing.Inc()
	tasksProcessedTotal.Inc()

	// Handle completion
	defer func() {
		tasksProcessing.Dec()
	}()

	// 2. Sleep for "value" milliseconds
	// We don't use time.Sleep to be able to cancel the sleep if the context is cancelled
	sleepDur := time.Duration(task.Value) * time.Millisecond
	select {
	case <-time.After(sleepDur):
	case <-ctx.Done():
		return ctx.Err()
	}

	// 3. Set to "done"
	if err := s.repo.UpdateTaskState(ctx, task.ID, "done"); err != nil {
		return fmt.Errorf("updating state to done: %w", err)
	}

	// Metrics
	tasksDoneTotal.Inc()
	typeStr := strconv.Itoa(int(task.Type))
	tasksByTypeTotal.WithLabelValues(typeStr).Inc()
	taskValueSumByType.WithLabelValues(typeStr).Add(float64(task.Value))

	// 4. Log total sum for this type
	totalSum, err := s.repo.SumProcessedValuesByType(ctx, int(task.Type))
	if err != nil {
		s.logger.Error("Failed to calculate total sum by type", "error", err, "type", task.Type)
	}

	s.logger.Info("Task processed",
		"id", task.ID,
		"type", task.Type,
		"value", task.Value,
		"total_sum_for_type", totalSum,
	)

	return nil
}

func (s *service) Start(ctx context.Context) error {
	s.logger.Info("Starting consumer service reconciliation loop")

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("Consumer service stopping, waiting for in-flight tasks...")
			s.wg.Wait()
			s.logger.Info("All in-flight tasks completed")
			return nil
		case <-ticker.C:
			s.reconcileTasks(ctx)
		}
	}
}

func (s *service) reconcileTasks(ctx context.Context) {
	tasks, err := s.repo.ListFailedTasks(ctx, 100)
	if err != nil {
		s.logger.Error("Failed to list pending tasks during reconciliation", "error", err)
		return
	}

	if len(tasks) > 0 {
		s.logger.Info("Found pending tasks for reconciliation", "count", len(tasks))
	}

	for _, task := range tasks {
		// Create a bounded goroutine or just process synchronously
		// Synchronously is fine because HandleTask applies rate limits
		s.wg.Add(1)
		go func(t persistence.Task) {
			defer s.wg.Done()
			if err := s.HandleTask(ctx, t); err != nil {
				// We don't error out completely, just log
				if err != context.Canceled {
					s.logger.Error("Failed to process reconciled task", "id", t.ID, "error", err)
				}
			}
		}(task)
	}
}
