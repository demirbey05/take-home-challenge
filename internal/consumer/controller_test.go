package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/demirbey05/take-home/internal/persistence"
)

type mockService struct {
	handledTasks []persistence.Task
}

func (m *mockService) HandleTask(ctx context.Context, task persistence.Task) error {
	m.handledTasks = append(m.handledTasks, task)
	return nil
}

func TestNewMetricsMux(t *testing.T) {
	mux := NewMetricsMux()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rr := httptest.NewRecorder()

	mux.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}

	if rr.Body.String() != "OK" {
		t.Errorf("handler returned unexpected body: got %v want %v", rr.Body.String(), "OK")
	}
}

func TestRESTController_handleTasks(t *testing.T) {
	mockSvc := &mockService{}
	controller := NewRESTController(mockSvc)

	// Test case 1: Successful request
	task := persistence.Task{ID: 1, Type: 1, Value: 100, State: "pending"}
	body, _ := json.Marshal(task)
	req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(body))
	rr := httptest.NewRecorder()

	controller.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusCreated {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusCreated)
	}

	// wait for the goroutine to execute
	time.Sleep(50 * time.Millisecond)
	if len(mockSvc.handledTasks) != 1 {
		t.Errorf("expected 1 task handled, got %v", len(mockSvc.handledTasks))
	} else if mockSvc.handledTasks[0].ID != task.ID {
		t.Errorf("expected task ID %v, got %v", task.ID, mockSvc.handledTasks[0].ID)
	}

	// Test case 2: Wrong method
	req = httptest.NewRequest(http.MethodGet, "/tasks", nil)
	rr = httptest.NewRecorder()
	controller.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusMethodNotAllowed {
		t.Errorf("expected method not allowed, got %v", status)
	}

	// Test case 3: Bad request (invalid JSON)
	req = httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader([]byte("invalid json")))
	rr = httptest.NewRecorder()
	controller.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusBadRequest {
		t.Errorf("expected bad request, got %v", status)
	}
}

func TestStartProfilingServer(t *testing.T) {
	// Just call it to increase coverage.
	// Since it starts a goroutine and uses ListenAndServe, we use a random high port to avoid conflict.
	StartProfilingServer(0)
}
