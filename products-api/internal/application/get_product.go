package application

import (
	"context"
	"strings"
	"products-api/internal/domain"
)

type GetProductUseCase struct {
	repo domain.ProductRepository
}

func NewGetProductUseCase(repo domain.ProductRepository) *GetProductUseCase {
	return &GetProductUseCase{repo: repo}
}

func (uc *GetProductUseCase) Execute(ctx context.Context, id, market string) (domain.Product, error) {
	id = strings.ToUpper(strings.TrimSpace(id))
	market = strings.ToUpper(strings.TrimSpace(market))

	if id == "" {
		return domain.Product{}, domain.ErrInvalidProductID
	}
	if !isValidMarket(market) {
		return domain.Product{}, domain.ErrInvalidMarket
	}

	return uc.repo.GetByIDAndMarket(ctx, id, market)
}

// isValidMarket valida el contrato 5.C: solo MX, CO o PE (sección 5 de Expecificaciones.md).
func isValidMarket(market string) bool {
	switch market {
	case string(domain.MarketMX), string(domain.MarketCO), string(domain.MarketPE):
		return true
	default:
		return false
	}
}
