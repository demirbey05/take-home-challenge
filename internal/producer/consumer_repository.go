package producer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/demirbey05/take-home/internal/persistence"
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

func (r *consumerRepository) SendTask(ctx context.Context, task persistence.Task) error {
	payload, err := json.Marshal(task)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.consumerURL+"/tasks", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code from consumer: %d", resp.StatusCode)
	}

	return nil
}
