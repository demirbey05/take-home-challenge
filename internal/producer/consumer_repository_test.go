package producer

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/demirbey05/take-home/internal/persistence"
	"github.com/stretchr/testify/assert"
)

func TestNewConsumerRepository(t *testing.T) {
	repo := NewConsumerRepository("http://localhost:8081")
	assert.NotNil(t, repo)
}

func TestIsRetryable(t *testing.T) {
	assert.True(t, isRetryable(http.StatusTooManyRequests))
	assert.True(t, isRetryable(http.StatusInternalServerError))
	assert.True(t, isRetryable(http.StatusServiceUnavailable))
	assert.False(t, isRetryable(http.StatusBadRequest))
	assert.False(t, isRetryable(http.StatusNotFound))
}

func TestRetryDelay(t *testing.T) {
	for attempt := 0; attempt < 10; attempt++ {
		delay := retryDelay(attempt)
		assert.GreaterOrEqual(t, delay, time.Duration(0))
		assert.LessOrEqual(t, delay, maxRetryDelay)
	}
}

func TestConsumerRepository_SendTask_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/tasks", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	repo := NewConsumerRepository(server.URL)
	err := repo.SendTask(context.Background(), persistence.Task{ID: 1})
	assert.NoError(t, err)
}

func TestConsumerRepository_SendTask_PermanentError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest) // Non-retryable
	}))
	defer server.Close()

	repo := NewConsumerRepository(server.URL)
	err := repo.SendTask(context.Background(), persistence.Task{ID: 1})
	assert.ErrorContains(t, err, "unexpected status code from consumer: 400")
}

func TestConsumerRepository_SendTask_RetryAndFail(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.WriteHeader(http.StatusInternalServerError) // Retryable
	}))
	defer server.Close()

	repo := NewConsumerRepository(server.URL)
	
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := repo.SendTask(ctx, persistence.Task{ID: 1})
	assert.Error(t, err)
	assert.Greater(t, attempts, 1)
}

func TestConsumerRepository_SendTask_ContextCancelled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError) // Retryable
	}))
	defer server.Close()

	repo := NewConsumerRepository(server.URL)
	
	ctx, cancel := context.WithCancel(context.Background())
	// cancel immediately to trigger the ctx.Done() case in retry backoff
	
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	err := repo.SendTask(ctx, persistence.Task{ID: 1})
	assert.ErrorContains(t, err, "send task cancelled during retry backoff")
}
