package producer

import (
	"context"
	"testing"

	"github.com/demirbey05/take-home/internal/persistence"
	"github.com/stretchr/testify/assert"
)

// mockQuerier implements persistence.Querier for testing
type mockQuerier struct {
	persistence.Querier
	createTaskFn        func(ctx context.Context, arg persistence.CreateTaskParams) (persistence.Task, error)
	countTasksByStateFn func(ctx context.Context, state string) (int64, error)
}

func (m *mockQuerier) CreateTask(ctx context.Context, arg persistence.CreateTaskParams) (persistence.Task, error) {
	if m.createTaskFn != nil {
		return m.createTaskFn(ctx, arg)
	}
	return persistence.Task{}, nil
}

func (m *mockQuerier) CountTasksByState(ctx context.Context, state string) (int64, error) {
	if m.countTasksByStateFn != nil {
		return m.countTasksByStateFn(ctx, state)
	}
	return 0, nil
}

func TestNewRepository(t *testing.T) {
	mq := &mockQuerier{}
	repo := NewRepository(mq)
	assert.NotNil(t, repo)
}

func TestRepository_CreateTask(t *testing.T) {
	mq := &mockQuerier{
		createTaskFn: func(ctx context.Context, arg persistence.CreateTaskParams) (persistence.Task, error) {
			return persistence.Task{
				ID:    1,
				Type:  arg.Type,
				Value: arg.Value,
			}, nil
		},
	}
	repo := NewRepository(mq)

	task, err := repo.CreateTask(context.Background(), 1, 100)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), task.ID)
	assert.Equal(t, int32(1), task.Type)
	assert.Equal(t, int32(100), task.Value)
}

func TestRepository_GetPendingTasksCount(t *testing.T) {
	mq := &mockQuerier{
		countTasksByStateFn: func(ctx context.Context, state string) (int64, error) {
			return 5, nil
		},
	}
	repo := NewRepository(mq)

	count, err := repo.GetPendingTasksCount(context.Background())
	assert.NoError(t, err)
	// 5 for received, 5 for failed
	assert.Equal(t, 10, count)
}

func TestRepository_CountTasksByState(t *testing.T) {
	mq := &mockQuerier{
		countTasksByStateFn: func(ctx context.Context, state string) (int64, error) {
			assert.Equal(t, "done", state)
			return 10, nil
		},
	}
	repo := NewRepository(mq)

	count, err := repo.CountTasksByState(context.Background(), "done")
	assert.NoError(t, err)
	assert.Equal(t, 10, count)
}
