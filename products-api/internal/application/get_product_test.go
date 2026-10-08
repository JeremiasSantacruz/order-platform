package application

import (
	"context"
	"errors"
	"testing"

	"products-api/internal/domain"
)

type fakeRepo struct {
	gotID     string
	gotMarket string
	calls     int
	product   domain.Product
	err       error
}

func (f *fakeRepo) GetByIDAndMarket(_ context.Context, id, market string) (domain.Product, error) {
	f.calls++
	f.gotID = id
	f.gotMarket = market
	return f.product, f.err
}

func TestGetProductUseCase_Success(t *testing.T) {
	want := domain.Product{
		ProductID:   "PRD-001",
		Name:        "Bebida 600 ml",
		Sku:         "BEB-600-PET",
		Status:      domain.StatusActive,
		TaxCategory: domain.TaxStandard,
		Market:      "MX",
	}
	repo := &fakeRepo{product: want}
	uc := NewGetProductUseCase(repo)

	got, err := uc.Execute(context.Background(), "PRD-001", "MX")
	if err != nil {
		t.Fatalf("Execute() error = %v, want nil", err)
	}
	if got != want {
		t.Errorf("Execute() = %+v, want %+v", got, want)
	}
	if repo.calls != 1 || repo.gotID != "PRD-001" || repo.gotMarket != "MX" {
		t.Errorf("repo calls = %d id=%q market=%q, want 1 id=PRD-001 market=MX",
			repo.calls, repo.gotID, repo.gotMarket)
	}
}

func TestGetProductUseCase_NormalizesInput(t *testing.T) {
	repo := &fakeRepo{}
	uc := NewGetProductUseCase(repo)

	if _, err := uc.Execute(context.Background(), "  prd-001 ", " mx "); err != nil {
		t.Fatalf("Execute() error = %v, want nil", err)
	}
	if repo.gotID != "PRD-001" || repo.gotMarket != "MX" {
		t.Errorf("normalization failed: id=%q market=%q, want PRD-001/MX", repo.gotID, repo.gotMarket)
	}
}

func TestGetProductUseCase_InvalidInputDoesNotHitRepo(t *testing.T) {
	tests := []struct {
		name    string
		id      string
		market  string
		wantErr error
	}{
		{"empty id", "", "MX", domain.ErrInvalidProductID},
		{"blank id", "   ", "MX", domain.ErrInvalidProductID},
		{"invalid market", "PRD-001", "AR", domain.ErrInvalidMarket},
		{"empty market", "PRD-001", "", domain.ErrInvalidMarket},
		{"lowercase invalid market", "PRD-001", "us", domain.ErrInvalidMarket},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &fakeRepo{}
			uc := NewGetProductUseCase(repo)

			_, err := uc.Execute(context.Background(), tt.id, tt.market)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Execute() error = %v, want %v", err, tt.wantErr)
			}
			if repo.calls != 0 {
				t.Errorf("repo should not be called for invalid input, got %d calls", repo.calls)
			}
		})
	}
}

func TestGetProductUseCase_PropagatesRepoError(t *testing.T) {
	repo := &fakeRepo{err: domain.ErrProductNotFound}
	uc := NewGetProductUseCase(repo)

	_, err := uc.Execute(context.Background(), "PRD-999", "MX")
	if !errors.Is(err, domain.ErrProductNotFound) {
		t.Errorf("Execute() error = %v, want ErrProductNotFound", err)
	}
}
