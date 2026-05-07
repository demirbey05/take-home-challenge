package consumer

import (
	"context"

	"github.com/demirbey05/take-home/internal/persistence"
)

type Repository interface {
	UpdateTaskState(ctx context.Context, id int64, state string) error
	SumProcessedValuesByType(ctx context.Context, taskType int) (int64, error)
	ListPendingTasks(ctx context.Context, limit int32) ([]persistence.Task, error)
}

type repository struct {
	q persistence.Querier
}

func NewRepository(q persistence.Querier) Repository {
	return &repository{q: q}
}

func (r *repository) UpdateTaskState(ctx context.Context, id int64, state string) error {
	return r.q.UpdateTaskState(ctx, persistence.UpdateTaskStateParams{
		ID:    id,
		State: state,
	})
}

func (r *repository) SumProcessedValuesByType(ctx context.Context, taskType int) (int64, error) {
	return r.q.SumProcessedValuesByType(ctx, int32(taskType))
}

func (r *repository) ListPendingTasks(ctx context.Context, limit int32) ([]persistence.Task, error) {
	return r.q.ListPendingTasks(ctx, limit)
}
