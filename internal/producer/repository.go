package producer

import (
	"context"

	"github.com/demirbey05/take-home/internal/persistence"
)

type Repository interface {
	CreateTask(ctx context.Context, taskType, value int) (persistence.Task, error)
	GetPendingTasksCount(ctx context.Context) (int, error)
}

type repository struct {
	q persistence.Querier
}

func NewRepository(q persistence.Querier) Repository {
	return &repository{q: q}
}

func (r *repository) CreateTask(ctx context.Context, taskType, value int) (persistence.Task, error) {
	return r.q.CreateTask(ctx, persistence.CreateTaskParams{
		Type:  int32(taskType),
		Value: int32(value),
	})
}

func (r *repository) GetPendingTasksCount(ctx context.Context) (int, error) {
	count, err := r.q.CountTasksByState(ctx, "received")
	return int(count), err
}
