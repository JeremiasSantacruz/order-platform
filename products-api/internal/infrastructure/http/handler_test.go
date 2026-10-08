package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"products-api/internal/application"
	"products-api/internal/infrastructure/memory"
)

func newTestMux() *http.ServeMux {
	repo := memory.NewProductRepository()
	handler := NewProductHandler(application.NewGetProductUseCase(repo))

	mux := http.NewServeMux()
	mux.HandleFunc("GET /products/{productId}", handler.GetProduct)
	return mux
}

func doRequest(t *testing.T, mux *http.ServeMux, target string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
	return rec
}

func TestGetProduct_SuccessContract5C(t *testing.T) {
	rec := doRequest(t, newTestMux(), "/products/PRD-001?market=MX")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response is not valid JSON: %v", err)
	}

	wantFields := []string{"productId", "name", "sku", "status", "taxCategory"}
	if len(body) != len(wantFields) {
		t.Errorf("field count = %d (%v), want exactly %d fields %v", len(body), keys(body), len(wantFields), wantFields)
	}
	for _, f := range wantFields {
		if _, ok := body[f]; !ok {
			t.Errorf("missing contract field %q", f)
		}
	}
	if _, ok := body["market"]; ok {
		t.Errorf("field %q must not be exposed (contract 5.C)", "market")
	}

	want := map[string]any{
		"productId":   "PRD-001",
		"name":        "Bebida 600 ml",
		"sku":         "BEB-600-PET",
		"status":      "ACTIVE",
		"taxCategory": "STANDARD",
	}
	for k, v := range want {
		if body[k] != v {
			t.Errorf("%s = %v, want %v", k, body[k], v)
		}
	}
}

func TestGetProduct_Errors(t *testing.T) {
	tests := []struct {
		name       string
		target     string
		wantStatus int
	}{
		{"product not found in market", "/products/PRD-001?market=CO", http.StatusNotFound},
		{"unknown product", "/products/PRD-999?market=MX", http.StatusNotFound},
		{"discontinued product still served (status is data)", "/products/PRD-003?market=PE", http.StatusOK},
		{"invalid market", "/products/PRD-001?market=AR", http.StatusBadRequest},
		{"missing market", "/products/PRD-001", http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := doRequest(t, newTestMux(), tt.target)
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", rec.Code, tt.wantStatus, rec.Body.String())
			}
			if tt.wantStatus == http.StatusOK {
				return
			}

			var errBody ErrorResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &errBody); err != nil {
				t.Fatalf("error body is not valid JSON: %v", err)
			}
			if errBody.Code != tt.wantStatus || errBody.Message == "" {
				t.Errorf("error body = %+v, want {code:%d, message:<non-empty>}", errBody, tt.wantStatus)
			}
		})
	}
}

func keys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
