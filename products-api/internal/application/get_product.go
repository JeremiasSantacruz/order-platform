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
	if market == "" {
		return domain.Product{}, domain.ErrInvalidMarket
	}

	return uc.repo.GetByIDAndMarket(ctx, id, market)
}
