package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newTestAPI(store *fakeStore, fr *fakeRedis) *API {
	logger := slog.New(slog.NewTextHandler(nopWriter{}, nil))
	svc := newTestService(store, fr)
	return NewAPI(svc, nil, logger)
}

type nopWriter struct{}

func (nopWriter) Write(p []byte) (int, error) { return len(p), nil }

func TestHandleGetProduct_ReturnsProductAndSource(t *testing.T) {
	store := newFakeStore(Product{ID: 1, Name: "Widget", PriceCent: 100})
	api := newTestAPI(store, newFakeRedis())

	req := httptest.NewRequest(http.MethodGet, "/products/1", nil)
	rec := httptest.NewRecorder()
	api.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body: %s", rec.Code, rec.Body.String())
	}
	var resp productResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON response: %v (body: %s)", err, rec.Body.String())
	}
	if resp.Product.Name != "Widget" {
		t.Errorf("product name = %q, want %q", resp.Product.Name, "Widget")
	}
	if resp.Source != SourceDatabase {
		t.Errorf("source = %q, want %q on first read", resp.Source, SourceDatabase)
	}
}

func TestHandleGetProduct_UnknownIDReturns404(t *testing.T) {
	store := newFakeStore()
	api := newTestAPI(store, newFakeRedis())

	req := httptest.NewRequest(http.MethodGet, "/products/999", nil)
	rec := httptest.NewRecorder()
	api.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
}

func TestHandleGetProduct_NonNumericIDReturns400(t *testing.T) {
	store := newFakeStore()
	api := newTestAPI(store, newFakeRedis())

	req := httptest.NewRequest(http.MethodGet, "/products/not-a-number", nil)
	rec := httptest.NewRecorder()
	api.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestHandleCreateProduct_CreatesAndReturns201(t *testing.T) {
	store := newFakeStore()
	api := newTestAPI(store, newFakeRedis())

	body := strings.NewReader(`{"name":"Gadget","price_cents":250}`)
	req := httptest.NewRequest(http.MethodPost, "/products", body)
	rec := httptest.NewRecorder()
	api.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201, body: %s", rec.Code, rec.Body.String())
	}
	var p Product
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("invalid JSON response: %v", err)
	}
	if p.Name != "Gadget" || p.ID == 0 {
		t.Errorf("created product = %+v, want a named product with a non-zero ID", p)
	}
}

func TestHandleCreateProduct_MissingNameReturns400(t *testing.T) {
	store := newFakeStore()
	api := newTestAPI(store, newFakeRedis())

	body := strings.NewReader(`{"price_cents":250}`)
	req := httptest.NewRequest(http.MethodPost, "/products", body)
	rec := httptest.NewRecorder()
	api.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestHandleUpdateProduct_UpdatesAndInvalidatesCache(t *testing.T) {
	store := newFakeStore(Product{ID: 1, Name: "Widget", PriceCent: 100})
	fr := newFakeRedis()
	api := newTestAPI(store, fr)

	// Prime the cache via a GET.
	getReq := httptest.NewRequest(http.MethodGet, "/products/1", nil)
	api.Routes().ServeHTTP(httptest.NewRecorder(), getReq)
	if !fr.has(productKey(1)) {
		t.Fatal("expected the GET to prime the cache")
	}

	body := strings.NewReader(`{"name":"Widget v2","price_cents":150}`)
	req := httptest.NewRequest(http.MethodPut, "/products/1", body)
	rec := httptest.NewRecorder()
	api.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body: %s", rec.Code, rec.Body.String())
	}
	if fr.has(productKey(1)) {
		t.Error("expected PUT to invalidate the cache entry")
	}
}

func TestHandleChaosEndpoints_DisabledByDefault(t *testing.T) {
	store := newFakeStore()
	api := newTestAPI(store, newFakeRedis()) // chaos is nil in newTestAPI

	for _, req := range []*http.Request{
		httptest.NewRequest(http.MethodPost, "/admin/nodes/1/kill", nil),
		httptest.NewRequest(http.MethodPost, "/admin/nodes/1/revive", nil),
	} {
		rec := httptest.NewRecorder()
		api.Routes().ServeHTTP(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Errorf("%s %s: status = %d, want 403 when chaos controls are disabled", req.Method, req.URL.Path, rec.Code)
		}
	}
}

func TestHandleChaosStatus_ReportsDisabledWhenChaosIsNil(t *testing.T) {
	store := newFakeStore()
	api := newTestAPI(store, newFakeRedis())

	req := httptest.NewRequest(http.MethodGet, "/admin/nodes", nil)
	rec := httptest.NewRecorder()
	api.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if enabled, _ := body["enabled"].(bool); enabled {
		t.Error("expected enabled=false when the API was constructed with a nil ChaosController")
	}
}

func TestHandleHealth_ReturnsOK(t *testing.T) {
	store := newFakeStore()
	api := newTestAPI(store, newFakeRedis())

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	api.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK || rec.Body.String() != "OK" {
		t.Errorf("status=%d body=%q, want 200 OK", rec.Code, rec.Body.String())
	}
}
