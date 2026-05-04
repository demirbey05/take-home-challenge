package producer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

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
	}

	err := s.processNext(context.Background())
	assert.NoError(t, err)

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
	}

	// Should not return an error because consumer failures are ignored in production loop
	err := s.processNext(context.Background())
	assert.NoError(t, err)

	repo.AssertExpectations(t)
	consRepo.AssertExpectations(t)
}
