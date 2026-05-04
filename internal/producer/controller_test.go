package producer

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestController_HealthCheck(t *testing.T) {
	c := NewController()
	
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rr := httptest.NewRecorder()

	c.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "OK", rr.Body.String())
}

func TestController_MetricsEndpoint(t *testing.T) {
	c := NewController()
	
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()

	c.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	// Ensure standard prometheus metrics are present
	assert.Contains(t, rr.Body.String(), "go_")
}
