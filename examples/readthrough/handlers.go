package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

// API wires Service (and, optionally, a ChaosController) to HTTP handlers.
// Uses Go 1.22's stdlib ServeMux method+wildcard routing, so this demo
// needs no router dependency beyond what's already in go.mod.
type API struct {
	service *Service
	chaos   *ChaosController // nil when chaos controls are disabled
	logger  *slog.Logger
}

func NewAPI(service *Service, chaos *ChaosController, logger *slog.Logger) *API {
	return &API{service: service, chaos: chaos, logger: logger}
}

func (a *API) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /products/{id}", a.handleGetProduct)
	mux.HandleFunc("POST /products", a.handleCreateProduct)
	mux.HandleFunc("PUT /products/{id}", a.handleUpdateProduct)
	mux.HandleFunc("POST /admin/nodes/{id}/kill", a.handleChaosKill)
	mux.HandleFunc("POST /admin/nodes/{id}/revive", a.handleChaosRevive)
	mux.HandleFunc("GET /admin/nodes", a.handleChaosStatus)
	mux.HandleFunc("GET /health", a.handleHealth)

	staticRoot, err := fs.Sub(staticFiles, "static")
	if err != nil {
		// staticFiles is compiled in via go:embed — a missing "static"
		// subdirectory would be a build-time packaging bug, not a
		// runtime condition to recover from.
		panic(fmt.Sprintf("readthrough: embedded static assets missing: %v", err))
	}
	mux.Handle("GET /", http.FileServer(http.FS(staticRoot)))
	return mux
}

func (a *API) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

type productResponse struct {
	Product   Product `json:"product"`
	Source    Source  `json:"source"`
	LatencyMS float64 `json:"latency_ms"`
}

func (a *API) handleGetProduct(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid product id")
		return
	}

	start := time.Now()
	p, source, err := a.service.GetProduct(r.Context(), id)
	elapsed := time.Since(start)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			writeError(w, http.StatusNotFound, "product not found")
			return
		}
		a.logger.Error("get product failed", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	writeJSON(w, http.StatusOK, productResponse{
		Product:   p,
		Source:    source,
		LatencyMS: float64(elapsed.Microseconds()) / 1000.0,
	})
}

type createProductRequest struct {
	Name      string `json:"name"`
	PriceCent int64  `json:"price_cents"`
}

func (a *API) handleCreateProduct(w http.ResponseWriter, r *http.Request) {
	var req createProductRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}

	created, err := a.service.CreateProduct(r.Context(), Product{Name: req.Name, PriceCent: req.PriceCent})
	if err != nil {
		a.logger.Error("create product failed", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusCreated, created)
}

func (a *API) handleUpdateProduct(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid product id")
		return
	}
	var req createProductRequest
	if err = json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	updated, err := a.service.UpdateProduct(r.Context(), Product{ID: id, Name: req.Name, PriceCent: req.PriceCent})
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			writeError(w, http.StatusNotFound, "product not found")
			return
		}
		a.logger.Error("update product failed", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, updated)
}

// handleChaosKill and handleChaosRevive are the "live kill-a-node control"
// this demo advertises: they shell out to docker stop/start on a
// HydraCache container. They only run at all if a is constructed with a
// non-nil ChaosController — see main.go's -enable-chaos-controls flag,
// off by default. Never wire this into anything that isn't a local demo.
func (a *API) handleChaosKill(w http.ResponseWriter, r *http.Request) {
	if a.chaos == nil {
		writeError(w, http.StatusForbidden, "chaos controls are disabled (start with -enable-chaos-controls to enable)")
		return
	}
	nodeID := r.PathValue("id")
	if err := a.chaos.Kill(r.Context(), nodeID); err != nil {
		a.logger.Error("chaos kill failed", "node", nodeID, "error", err)
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"node": nodeID, "action": "killed"})
}

func (a *API) handleChaosRevive(w http.ResponseWriter, r *http.Request) {
	if a.chaos == nil {
		writeError(w, http.StatusForbidden, "chaos controls are disabled (start with -enable-chaos-controls to enable)")
		return
	}
	nodeID := r.PathValue("id")
	if err := a.chaos.Revive(r.Context(), nodeID); err != nil {
		a.logger.Error("chaos revive failed", "node", nodeID, "error", err)
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"node": nodeID, "action": "revived"})
}

func (a *API) handleChaosStatus(w http.ResponseWriter, r *http.Request) {
	if a.chaos == nil {
		writeJSON(w, http.StatusOK, map[string]any{"enabled": false, "nodes": []string{}})
		return
	}
	statuses := a.chaos.Status(r.Context())
	writeJSON(w, http.StatusOK, map[string]any{"enabled": true, "nodes": statuses})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
