package producer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/demirbey05/take-home/internal/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	tasksProducedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "producer_tasks_produced_total",
		Help: "The total number of produced tasks",
	})

	tasksStateReceived = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "producer_tasks_state_received",
		Help: "Current number of tasks in received state",
	})
	tasksStateProcessing = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "producer_tasks_state_processing",
		Help: "Current number of tasks in processing state",
	})
	tasksStateDone = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "producer_tasks_state_done",
		Help: "Current number of tasks in done state",
	})

	tasksSendFailuresTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "producer_tasks_send_failures_total",
		Help: "Total number of tasks that failed to be sent to consumer after all retries",
	})
)

type Service interface {
	Start(ctx context.Context) error
}

const maxInflightSends = 10

type service struct {
	cfg      *config.Config
	repo     Repository
	consRepo ConsumerRepository
	logger   *slog.Logger
	wg       sync.WaitGroup
	sendSem  chan struct{} // bounds concurrent in-flight sends
}

func NewService(cfg *config.Config, repo Repository, consRepo ConsumerRepository, logger *slog.Logger) Service {
	return &service{
		cfg:      cfg,
		repo:     repo,
		consRepo: consRepo,
		logger:   logger,
		sendSem:  make(chan struct{}, maxInflightSends),
	}
}

func (s *service) Start(ctx context.Context) error {
	s.logger.Info("Starting producer service", "rate_per_sec", s.cfg.ProduceRatePerSec, "max_backlog", s.cfg.MaxBacklog)

	// Start background goroutine that periodically refreshes per-state gauges
	go s.startStateGaugeUpdater(ctx)

	rateDuration := time.Second
	if s.cfg.ProduceRatePerSec > 0 {
		rateDuration = time.Duration(1000/s.cfg.ProduceRatePerSec) * time.Millisecond
	}

	ticker := time.NewTicker(rateDuration)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("Producer service stopping, waiting for in-flight sends...")
			s.wg.Wait()
			s.logger.Info("All in-flight sends completed")
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
	taskType := rand.Intn(10)   // 0-9
	taskValue := rand.Intn(100) // 0-99

	// Persist
	task, err := s.repo.CreateTask(ctx, taskType, taskValue)
	if err != nil {
		return fmt.Errorf("creating task: %w", err)
	}

	tasksProducedTotal.Inc()

	s.logger.Debug("Created task", "id", task.ID, "type", task.Type, "value", task.Value)

	// Send to consumer asynchronously to avoid blocking production while retries are in-flight.
	// The semaphore bounds concurrent sends; if the consumer is down we don't spawn unlimited goroutines.
	// Wait group is for graceful shutdown of the producer service.

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		// Acquire semaphore slot — provides backpressure when many sends are in-flight
		select {
		case s.sendSem <- struct{}{}:
			defer func() { <-s.sendSem }()
		case <-ctx.Done():
			s.logger.Debug("Send cancelled before acquiring slot", "id", task.ID)
			return
		}

		if err := s.consRepo.SendTask(ctx, task); err != nil {
			s.logger.Warn("Failed to send task to consumer after retries",
				"id", task.ID, "error", err)
			tasksSendFailuresTotal.Inc() // Atomic safe
			// Task stays in "received" state in DB.
			// The consumer can pick it up via ListPendingTasks reconciliation.
		} else {
			s.logger.Debug("Sent task to consumer", "id", task.ID)
		}
	}()

	return nil
}

// startStateGaugeUpdater periodically queries the database for task counts
// per state and updates the corresponding Prometheus gauges.
func (s *service) startStateGaugeUpdater(ctx context.Context) {
	const refreshInterval = 2 * time.Second
	ticker := time.NewTicker(refreshInterval)
	defer ticker.Stop()

	s.logger.Info("Starting state gauge updater", "interval", refreshInterval)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.refreshStateGauges(ctx)
		}
	}
}

func (s *service) refreshStateGauges(ctx context.Context) {
	states := []struct {
		name  string
		gauge prometheus.Gauge
	}{
		{"received", tasksStateReceived},
		{"processing", tasksStateProcessing},
		{"done", tasksStateDone},
	}

	for _, st := range states {
		count, err := s.repo.CountTasksByState(ctx, st.name)
		if err != nil {
			s.logger.Error("Failed to refresh state gauge", "state", st.name, "error", err)
			continue
		}
		st.gauge.Set(float64(count))
	}
}
