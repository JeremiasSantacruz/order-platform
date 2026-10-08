package memory

import (
	"context"
	"errors"
	"testing"

	"products-api/internal/domain"
)

func TestNewProductRepository_SeedsMatchSpecExamples(t *testing.T) {
	repo := NewProductRepository()

	tests := []struct {
		id         string
		market     string
		wantName   string
		wantStatus domain.Status
		wantTax    domain.TaxCategory
	}{
		{"PRD-001", "MX", "Bebida 600 ml", domain.StatusActive, domain.TaxStandard},
		{"PRD-008", "MX", "Cereal Avena 500g", domain.StatusActive, domain.TaxStandard},
		{"PRD-003", "PE", "Galletas Chocolate 200g", domain.StatusDiscontinued, domain.TaxStandard},
		{"PRD-002", "CO", "Agua Mineral 1.5L", domain.StatusActive, domain.TaxExempt},
		{"PRD-007", "MX", "Jugo Naranja 1L", domain.StatusActive, domain.TaxReduced},
	}
	for _, tt := range tests {
		t.Run(tt.id+"/"+tt.market, func(t *testing.T) {
			p, err := repo.GetByIDAndMarket(context.Background(), tt.id, tt.market)
			if err != nil {
				t.Fatalf("GetByIDAndMarket() error = %v", err)
			}
			if p.ProductID != tt.id || p.Market != tt.market {
				t.Errorf("got %s/%s, want %s/%s", p.ProductID, p.Market, tt.id, tt.market)
			}
			if p.Name != tt.wantName || p.Status != tt.wantStatus || p.TaxCategory != tt.wantTax {
				t.Errorf("got name=%q status=%q tax=%q, want name=%q status=%q tax=%q",
					p.Name, p.Status, p.TaxCategory, tt.wantName, tt.wantStatus, tt.wantTax)
			}
		})
	}
}

func TestGetByIDAndMarket_NotFound(t *testing.T) {
	repo := NewProductRepository()

	tests := []struct {
		name string
		id   string
		mark string
	}{
		{"unknown product", "PRD-999", "MX"},
		{"product exists in another market", "PRD-001", "CO"},
		{"exact match only", "prd-001", "MX"},
		{"wrong market code", "PRD-001", "mx"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := repo.GetByIDAndMarket(context.Background(), tt.id, tt.mark)
			if !errors.Is(err, domain.ErrProductNotFound) {
				t.Errorf("error = %v, want ErrProductNotFound", err)
			}
		})
	}
}

func TestGetByIDAndMarket_CanceledContext(t *testing.T) {
	repo := NewProductRepository()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := repo.GetByIDAndMarket(ctx, "PRD-001", "MX")
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want context.Canceled", err)
	}
}
