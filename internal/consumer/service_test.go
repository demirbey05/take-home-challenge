package consumer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/demirbey05/take-home/internal/config"
	"github.com/demirbey05/take-home/internal/persistence"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"golang.org/x/time/rate"
)

// ---- Mock Repository ----

type mockRepository struct {
	mock.Mock
}

func (m *mockRepository) UpdateTaskState(ctx context.Context, id int64, state string) error {
	args := m.Called(ctx, id, state)
	return args.Error(0)
}

func (m *mockRepository) ListFailedTasks(ctx context.Context, limit int32) ([]persistence.Task, error) {
	args := m.Called(ctx, limit)
	return args.Get(0).([]persistence.Task), args.Error(1)
}

func (m *mockRepository) SumProcessedValuesByType(ctx context.Context, taskType int) (int64, error) {
	args := m.Called(ctx, taskType)
	return args.Get(0).(int64), args.Error(1)
}

// ---- Helpers ----

// newTestService builds a service with a fast rate limiter so tests don't block.
func newTestService(repo Repository) *service {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &service{
		cfg:     &config.Config{RateLimitPerSec: 1000},
		repo:    repo,
		logger:  logger,
		limiter: rate.NewLimiter(rate.Limit(1000), 1000),
	}
}

// sampleTask returns a task with a tiny Value so the simulated sleep is negligible.
func sampleTask() persistence.Task {
	return persistence.Task{ID: 1, Type: 3, Value: 1, State: "received"}
}

// ---- Tests ----

func TestHandleTask_Success(t *testing.T) {
	repo := new(mockRepository)
	s := newTestService(repo)
	task := sampleTask()

	repo.On("UpdateTaskState", mock.Anything, task.ID, "processing").Return(nil)
	repo.On("UpdateTaskState", mock.Anything, task.ID, "done").Return(nil)
	repo.On("SumProcessedValuesByType", mock.Anything, int(task.Type)).Return(int64(42), nil)

	err := s.HandleTask(context.Background(), task)

	assert.NoError(t, err)
	repo.AssertExpectations(t)
}

func TestHandleTask_UpdateToProcessingFails(t *testing.T) {
	repo := new(mockRepository)
	s := newTestService(repo)
	task := sampleTask()

	dbErr := errors.New("connection refused")
	repo.On("UpdateTaskState", mock.Anything, task.ID, "processing").Return(dbErr)

	err := s.HandleTask(context.Background(), task)

	assert.Error(t, err)
	assert.ErrorIs(t, err, dbErr)
	assert.Contains(t, err.Error(), "updating state to processing")

	// "done" and SumProcessedValuesByType must NOT have been called
	repo.AssertNotCalled(t, "UpdateTaskState", mock.Anything, task.ID, "done")
	repo.AssertNotCalled(t, "SumProcessedValuesByType", mock.Anything, mock.Anything)
	repo.AssertExpectations(t)
}

func TestHandleTask_UpdateToDoneFails(t *testing.T) {
	repo := new(mockRepository)
	s := newTestService(repo)
	task := sampleTask()

	dbErr := errors.New("serialization failure")
	repo.On("UpdateTaskState", mock.Anything, task.ID, "processing").Return(nil)
	repo.On("UpdateTaskState", mock.Anything, task.ID, "done").Return(dbErr)

	err := s.HandleTask(context.Background(), task)

	assert.Error(t, err)
	assert.ErrorIs(t, err, dbErr)
	assert.Contains(t, err.Error(), "updating state to done")

	// SumProcessedValuesByType must NOT have been called
	repo.AssertNotCalled(t, "SumProcessedValuesByType", mock.Anything, mock.Anything)
	repo.AssertExpectations(t)
}

func TestHandleTask_ContextCancelledDuringSleep(t *testing.T) {
	repo := new(mockRepository)
	s := newTestService(repo)
	// Use a large Value so we have time to cancel the context during the sleep.
	task := persistence.Task{ID: 2, Type: 1, Value: 5000, State: "received"}

	repo.On("UpdateTaskState", mock.Anything, task.ID, "processing").Return(nil)

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel after a short delay — well before the 5000ms sleep completes.
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := s.HandleTask(ctx, task)
	elapsed := time.Since(start)

	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	// Verify we exited early, not after the full 5s sleep.
	assert.Less(t, elapsed, 1*time.Second)

	// "done" must NOT have been called — we bailed out during the sleep.
	repo.AssertNotCalled(t, "UpdateTaskState", mock.Anything, task.ID, "done")
	repo.AssertNotCalled(t, "SumProcessedValuesByType", mock.Anything, mock.Anything)
	repo.AssertExpectations(t)
}

func TestHandleTask_SumProcessedValuesFails_NonFatal(t *testing.T) {
	repo := new(mockRepository)
	s := newTestService(repo)
	task := sampleTask()

	repo.On("UpdateTaskState", mock.Anything, task.ID, "processing").Return(nil)
	repo.On("UpdateTaskState", mock.Anything, task.ID, "done").Return(nil)
	// SumProcessedValuesByType fails — this should NOT cause HandleTask to return an error.
	repo.On("SumProcessedValuesByType", mock.Anything, int(task.Type)).
		Return(int64(0), errors.New("query timeout"))

	err := s.HandleTask(context.Background(), task)

	assert.NoError(t, err)
	repo.AssertExpectations(t)
}

func TestHandleTask_ZeroValueTask(t *testing.T) {
	repo := new(mockRepository)
	s := newTestService(repo)
	// Value == 0 means zero-millisecond sleep; should complete instantly.
	task := persistence.Task{ID: 3, Type: 0, Value: 0, State: "received"}

	repo.On("UpdateTaskState", mock.Anything, task.ID, "processing").Return(nil)
	repo.On("UpdateTaskState", mock.Anything, task.ID, "done").Return(nil)
	repo.On("SumProcessedValuesByType", mock.Anything, int(task.Type)).Return(int64(0), nil)

	start := time.Now()
	err := s.HandleTask(context.Background(), task)
	elapsed := time.Since(start)

	assert.NoError(t, err)
	assert.Less(t, elapsed, 100*time.Millisecond)
	repo.AssertExpectations(t)
}

func TestHandleTask_RateLimiterCancelled(t *testing.T) {
	repo := new(mockRepository)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	// Create a very restrictive limiter: 1 token/sec, burst 0 → Wait always blocks.
	s := &service{
		cfg:     &config.Config{RateLimitPerSec: 1},
		repo:    repo,
		logger:  logger,
		limiter: rate.NewLimiter(rate.Limit(1), 0),
	}
	task := sampleTask()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := s.HandleTask(ctx, task)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "rate limiter wait")
	// No repo calls should have been made.
	repo.AssertNotCalled(t, "UpdateTaskState", mock.Anything, mock.Anything, mock.Anything)
	repo.AssertNotCalled(t, "SumProcessedValuesByType", mock.Anything, mock.Anything)
}

func TestNewService_DefaultRateLimit(t *testing.T) {
	repo := new(mockRepository)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	cfg := &config.Config{RateLimitPerSec: 0} // invalid → should fall back to 20
	svc := NewService(cfg, repo, logger)

	// Verify the service was created (non-nil) and that it works.
	assert.NotNil(t, svc)

	// Use a valid task to exercise the fallback limiter.
	task := sampleTask()
	repo.On("UpdateTaskState", mock.Anything, task.ID, "processing").Return(nil)
	repo.On("UpdateTaskState", mock.Anything, task.ID, "done").Return(nil)
	repo.On("SumProcessedValuesByType", mock.Anything, int(task.Type)).Return(int64(0), nil)

	err := svc.HandleTask(context.Background(), task)
	assert.NoError(t, err)
	repo.AssertExpectations(t)
}

func TestReconcileTasks_Success(t *testing.T) {
	repo := new(mockRepository)
	s := newTestService(repo)

	task1 := persistence.Task{ID: 10, Type: 1, Value: 10, State: "received"}
	task2 := persistence.Task{ID: 11, Type: 2, Value: 20, State: "received"}

	// Mock ListPendingTasks to return our tasks
	repo.On("ListPendingTasks", mock.Anything, int32(100)).Return([]persistence.Task{task1, task2}, nil)

	// Mock HandleTask requirements for task1
	repo.On("UpdateTaskState", mock.Anything, task1.ID, "processing").Return(nil)
	repo.On("UpdateTaskState", mock.Anything, task1.ID, "done").Return(nil)
	repo.On("SumProcessedValuesByType", mock.Anything, int(task1.Type)).Return(int64(100), nil)

	// Mock HandleTask requirements for task2
	repo.On("UpdateTaskState", mock.Anything, task2.ID, "processing").Return(nil)
	repo.On("UpdateTaskState", mock.Anything, task2.ID, "done").Return(nil)
	repo.On("SumProcessedValuesByType", mock.Anything, int(task2.Type)).Return(int64(200), nil)

	// Run reconciliation
	s.reconcileTasks(context.Background())

	// Wait for the spawned goroutines to finish
	s.wg.Wait()

	repo.AssertExpectations(t)
}

func TestStart_Lifecycle(t *testing.T) {
	repo := new(mockRepository)
	s := newTestService(repo)

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error)
	go func() {
		errCh <- s.Start(ctx)
	}()

	// Cancel immediately to trigger shutdown
	cancel()

	// Wait for Start to return
	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(1 * time.Second):
		t.Fatal("Start did not return in time after context cancellation")
	}

	repo.AssertExpectations(t)
}
