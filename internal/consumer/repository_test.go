package consumer

import (
	"context"
	"testing"

	"github.com/demirbey05/take-home/internal/persistence"
	"github.com/stretchr/testify/assert"
)

// mockQuerier implements persistence.Querier for testing
type mockQuerier struct {
	persistence.Querier
	updateTaskStateFn          func(ctx context.Context, arg persistence.UpdateTaskStateParams) error
	sumProcessedValuesByTypeFn func(ctx context.Context, type_ int32) (int64, error)
}

func (m *mockQuerier) UpdateTaskState(ctx context.Context, arg persistence.UpdateTaskStateParams) error {
	if m.updateTaskStateFn != nil {
		return m.updateTaskStateFn(ctx, arg)
	}
	return nil
}

func (m *mockQuerier) SumProcessedValuesByType(ctx context.Context, type_ int32) (int64, error) {
	if m.sumProcessedValuesByTypeFn != nil {
		return m.sumProcessedValuesByTypeFn(ctx, type_)
	}
	return 0, nil
}

func TestNewRepository(t *testing.T) {
	mq := &mockQuerier{}
	repo := NewRepository(mq)
	assert.NotNil(t, repo)
}

func TestRepository_UpdateTaskState(t *testing.T) {
	mq := &mockQuerier{
		updateTaskStateFn: func(ctx context.Context, arg persistence.UpdateTaskStateParams) error {
			assert.Equal(t, int64(1), arg.ID)
			assert.Equal(t, "processing", arg.State)
			return nil
		},
	}
	repo := NewRepository(mq)

	err := repo.UpdateTaskState(context.Background(), 1, "processing")
	assert.NoError(t, err)
}

func TestRepository_SumProcessedValuesByType(t *testing.T) {
	mq := &mockQuerier{
		sumProcessedValuesByTypeFn: func(ctx context.Context, type_ int32) (int64, error) {
			assert.Equal(t, int32(2), type_)
			return 150, nil
		},
	}
	repo := NewRepository(mq)

	sum, err := repo.SumProcessedValuesByType(context.Background(), 2)
	assert.NoError(t, err)
	assert.Equal(t, int64(150), sum)
}
