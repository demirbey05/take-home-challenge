package producer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"github.com/demirbey05/take-home/internal/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	tasksProducedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tasks_produced_total",
		Help: "The total number of produced tasks",
	})
)

type Service interface {
	Start(ctx context.Context) error
}

type service struct {
	cfg      *config.Config
	repo     Repository
	consRepo ConsumerRepository
	logger   *slog.Logger
}

func NewService(cfg *config.Config, repo Repository, consRepo ConsumerRepository, logger *slog.Logger) Service {
	return &service{
		cfg:      cfg,
		repo:     repo,
		consRepo: consRepo,
		logger:   logger,
	}
}

func (s *service) Start(ctx context.Context) error {
	s.logger.Info("Starting producer service", "rate_per_sec", s.cfg.ProduceRatePerSec, "max_backlog", s.cfg.MaxBacklog)
	
	rateDuration := time.Second
	if s.cfg.ProduceRatePerSec > 0 {
		rateDuration = time.Duration(1000/s.cfg.ProduceRatePerSec) * time.Millisecond
	}
	
	ticker := time.NewTicker(rateDuration)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("Producer service stopping")
			return nil
		case <-ticker.C:
			if err := s.processNext(ctx); err != nil {
				if errors.Is(err, errMaxBacklogReached) {
					s.logger.Info("Max backlog reached, pausing production")
					// wait a bit longer when backlog is full
					time.Sleep(1 * time.Second)
				} else {
					s.logger.Error("Error processing next task", "error", err)
				}
			}
		}
	}
}

var errMaxBacklogReached = errors.New("max backlog reached")

func (s *service) processNext(ctx context.Context) error {
	// Check backlog
	pendingCount, err := s.repo.GetPendingTasksCount(ctx)
	if err != nil {
		return fmt.Errorf("getting pending count: %w", err)
	}

	if pendingCount >= s.cfg.MaxBacklog {
		return errMaxBacklogReached
	}

	// Generate task
	taskType := rand.Intn(10) // 0-9
	taskValue := rand.Intn(100) // 0-99

	// Persist
	task, err := s.repo.CreateTask(ctx, taskType, taskValue)
	if err != nil {
		return fmt.Errorf("creating task: %w", err)
	}

	tasksProducedTotal.Inc()

	s.logger.Debug("Created task", "id", task.ID, "type", task.Type, "value", task.Value)

	// Send to consumer
	if err := s.consRepo.SendTask(ctx, task); err != nil {
		s.logger.Warn("Failed to send task to consumer", "id", task.ID, "error", err)
		// We don't fail the production process, the consumer can poll/pull or we can implement retry later.
		// For now, it stays in "received" state in DB.
	} else {
		s.logger.Debug("Sent task to consumer", "id", task.ID)
	}

	return nil
}


