package producer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	"github.com/demirbey05/take-home/internal/persistence"
)

const (
	maxRetries      = 5
	baseRetryDelay  = 200 * time.Millisecond
	maxRetryDelay   = 30 * time.Second
)

type ConsumerRepository interface {
	SendTask(ctx context.Context, task persistence.Task) error
}

type consumerRepository struct {
	consumerURL string
	httpClient  *http.Client
}

func NewConsumerRepository(consumerURL string) ConsumerRepository {
	return &consumerRepository{
		consumerURL: consumerURL,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

// isRetryable returns true for errors that are transient and worth retrying:
// network errors, 5xx server errors, and 429 Too Many Requests.
// 4xx client errors (except 429) are permanent and not retried.
func isRetryable(statusCode int) bool {
	return statusCode == http.StatusTooManyRequests || statusCode >= http.StatusInternalServerError
}

// retryDelay computes the backoff duration for a given attempt using
// exponential backoff with full jitter to avoid thundering-herd / DDoS effects.
//
//	delay = random(0, min(maxRetryDelay, baseRetryDelay * 2^attempt))
func retryDelay(attempt int) time.Duration {
	// Exponential cap: baseRetryDelay * 2^attempt
	exp := baseRetryDelay * (1 << attempt)
	if exp > maxRetryDelay || exp <= 0 { // guard against int overflow
		exp = maxRetryDelay
	}
	// Full jitter: pick a random duration in [0, exp)
	return time.Duration(rand.Int63n(int64(exp)))
}

func (r *consumerRepository) SendTask(ctx context.Context, task persistence.Task) error {
	payload, err := json.Marshal(task)
	if err != nil {
		return err // marshalling errors are permanent; no retry
	}

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		// Build a fresh request each attempt (body reader is consumed after first use)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.consumerURL+"/tasks", bytes.NewReader(payload))
		if err != nil {
			return err // request construction errors are permanent; no retry
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := r.httpClient.Do(req)
		if err != nil {
			// Network / timeout error — transient, schedule a retry
			lastErr = fmt.Errorf("attempt %d: http do: %w", attempt+1, err)
		} else {
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusOK {
				return nil // success
			}
			lastErr = fmt.Errorf("attempt %d: unexpected status code from consumer: %d", attempt+1, resp.StatusCode)
			if !isRetryable(resp.StatusCode) {
				return lastErr // permanent client error; no retry
			}
		}

		// Wait before next attempt, but honour context cancellation
		delay := retryDelay(attempt)
		select {
		case <-ctx.Done():
			return fmt.Errorf("send task cancelled during retry backoff: %w", ctx.Err())
		case <-time.After(delay):
		}
	}

	return fmt.Errorf("send task failed after %d attempts: %w", maxRetries, lastErr)
}
