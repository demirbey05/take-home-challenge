package producer

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
)

// Mock DB Repository
type mockRepository struct {
	mock.Mock
}

func (m *mockRepository) CreateTask(ctx context.Context, taskType, value int) (persistence.Task, error) {
	args := m.Called(ctx, taskType, value)
	return args.Get(0).(persistence.Task), args.Error(1)
}

func (m *mockRepository) GetPendingTasksCount(ctx context.Context) (int, error) {
	args := m.Called(ctx)
	return args.Int(0), args.Error(1)
}

func (m *mockRepository) CountTasksByState(ctx context.Context, state string) (int, error) {
	args := m.Called(ctx, state)
	return args.Int(0), args.Error(1)
}

// Mock Consumer Repository
type mockConsumerRepository struct {
	mock.Mock
}

func (m *mockConsumerRepository) SendTask(ctx context.Context, task persistence.Task) error {
	args := m.Called(ctx, task)
	return args.Error(0)
}

func TestProcessNext_MaxBacklogReached(t *testing.T) {
	cfg := &config.Config{MaxBacklog: 10}
	repo := new(mockRepository)
	consRepo := new(mockConsumerRepository)
	// discard logs in tests
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Mock backlog > max
	repo.On("GetPendingTasksCount", mock.Anything).Return(10, nil)

	s := &service{
		cfg:      cfg,
		repo:     repo,
		consRepo: consRepo,
		logger:   logger,
		sendSem:  make(chan struct{}, maxInflightSends),
	}

	err := s.processNext(context.Background())
	assert.ErrorIs(t, err, errMaxBacklogReached)

	repo.AssertExpectations(t)
	consRepo.AssertExpectations(t)
}

func TestProcessNext_Success(t *testing.T) {
	cfg := &config.Config{MaxBacklog: 10}
	repo := new(mockRepository)
	consRepo := new(mockConsumerRepository)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Mock backlog ok
	repo.On("GetPendingTasksCount", mock.Anything).Return(5, nil)

	// Mock DB create
	mockTask := persistence.Task{ID: 1, Type: 5, Value: 50, State: "received"}
	// We use mock.Anything for type/value since they are generated randomly
	repo.On("CreateTask", mock.Anything, mock.Anything, mock.Anything).Return(mockTask, nil)

	// Mock Consumer Send
	consRepo.On("SendTask", mock.Anything, mockTask).Return(nil)

	s := &service{
		cfg:      cfg,
		repo:     repo,
		consRepo: consRepo,
		logger:   logger,
		sendSem:  make(chan struct{}, maxInflightSends),
	}

	err := s.processNext(context.Background())
	assert.NoError(t, err)

	// Wait for async send goroutine to complete
	s.wg.Wait()

	repo.AssertExpectations(t)
	consRepo.AssertExpectations(t)
}

func TestProcessNext_ConsumerSendFails(t *testing.T) {
	cfg := &config.Config{MaxBacklog: 10}
	repo := new(mockRepository)
	consRepo := new(mockConsumerRepository)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Mock backlog ok
	repo.On("GetPendingTasksCount", mock.Anything).Return(5, nil)

	// Mock DB create
	mockTask := persistence.Task{ID: 2, Type: 2, Value: 20, State: "received"}
	repo.On("CreateTask", mock.Anything, mock.Anything, mock.Anything).Return(mockTask, nil)

	// Mock Consumer Send fails
	consRepo.On("SendTask", mock.Anything, mockTask).Return(errors.New("network error"))

	s := &service{
		cfg:      cfg,
		repo:     repo,
		consRepo: consRepo,
		logger:   logger,
		sendSem:  make(chan struct{}, maxInflightSends),
	}

	// Should not return an error because consumer failures are handled asynchronously
	err := s.processNext(context.Background())
	assert.NoError(t, err)

	// Wait for async send goroutine to complete
	s.wg.Wait()

	// Give a moment for mock expectations to settle
	time.Sleep(10 * time.Millisecond)

	repo.AssertExpectations(t)
	consRepo.AssertExpectations(t)
}

func TestProcessNext_GetPendingError(t *testing.T) {
	cfg := &config.Config{MaxBacklog: 10}
	repo := new(mockRepository)
	consRepo := new(mockConsumerRepository)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	repo.On("GetPendingTasksCount", mock.Anything).Return(0, errors.New("db error"))

	s := &service{
		cfg:      cfg,
		repo:     repo,
		consRepo: consRepo,
		logger:   logger,
		sendSem:  make(chan struct{}, maxInflightSends),
	}

	err := s.processNext(context.Background())
	assert.ErrorContains(t, err, "getting pending count: db error")
}

func TestProcessNext_CreateTaskError(t *testing.T) {
	cfg := &config.Config{MaxBacklog: 10}
	repo := new(mockRepository)
	consRepo := new(mockConsumerRepository)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	repo.On("GetPendingTasksCount", mock.Anything).Return(5, nil)
	repo.On("CreateTask", mock.Anything, mock.Anything, mock.Anything).Return(persistence.Task{}, errors.New("create error"))

	s := &service{
		cfg:      cfg,
		repo:     repo,
		consRepo: consRepo,
		logger:   logger,
		sendSem:  make(chan struct{}, maxInflightSends),
	}

	err := s.processNext(context.Background())
	assert.ErrorContains(t, err, "creating task: create error")
}

func TestNewService(t *testing.T) {
	cfg := &config.Config{ProduceRatePerSec: 10, MaxBacklog: 100}
	repo := new(mockRepository)
	consRepo := new(mockConsumerRepository)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	s := NewService(cfg, repo, consRepo, logger)
	assert.NotNil(t, s)
}

func TestStart(t *testing.T) {
	cfg := &config.Config{ProduceRatePerSec: 100, MaxBacklog: 10}
	repo := new(mockRepository)
	consRepo := new(mockConsumerRepository)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Mock backlog ok
	repo.On("GetPendingTasksCount", mock.Anything).Return(5, nil).Maybe()
	repo.On("CreateTask", mock.Anything, mock.Anything, mock.Anything).Return(persistence.Task{ID: 1}, nil).Maybe()
	consRepo.On("SendTask", mock.Anything, mock.Anything).Return(nil).Maybe()

	s := NewService(cfg, repo, consRepo, logger)

	ctx, cancel := context.WithCancel(context.Background())
	
	// start in background and cancel immediately
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	err := s.Start(ctx)
	assert.NoError(t, err)
}
