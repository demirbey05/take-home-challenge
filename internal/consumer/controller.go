package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/pprof"

	"github.com/demirbey05/take-home/internal/persistence"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func NewMetricsMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	return mux
}

type RESTController struct {
	mux     *http.ServeMux
	service Service
}

func NewRESTController(service Service) *RESTController {
	mux := http.NewServeMux()
	c := &RESTController{
		mux:     mux,
		service: service,
	}
	mux.HandleFunc("/tasks", c.handleTasks)
	return c
}

func (c *RESTController) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c.mux.ServeHTTP(w, r)
}

func (c *RESTController) handleTasks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var task persistence.Task
	if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	go func() {
		_ = c.service.HandleTask(context.Background(), task)
	}()

	w.WriteHeader(http.StatusCreated)
}

// StartProfilingServer starts a separate HTTP server for pprof profiling
func StartProfilingServer(port int) {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	addr := fmt.Sprintf(":%d", port)
	go func() {
		_ = http.ListenAndServe(addr, mux)
	}()
}
