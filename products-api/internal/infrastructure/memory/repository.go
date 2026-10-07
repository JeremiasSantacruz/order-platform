package memory

import (
	"context"

	"products-api/internal/domain"
)

type ProductRepository struct {
	products []domain.Product
}

func NewProductRepository() *ProductRepository {
	// Semilla con 10 productos distribuidos en los 3 mercados soportados (MX, CO, PE).
	// PRD-001 y PRD-008 (STANDARD/ACTIVE en MX) reproducen el ejemplo del evento de la
	// sección 5.A y sus totales de la sección 5.D.
	seed := []domain.Product{
		// Mercado MX (México)
		{ProductID: "PRD-001", Name: "Bebida 600 ml", Sku: "BEB-600-PET", Status: domain.StatusActive, TaxCategory: domain.TaxStandard, Market: "MX"},
		{ProductID: "PRD-008", Name: "Cereal Avena 500g", Sku: "CER-AVE-500", Status: domain.StatusActive, TaxCategory: domain.TaxStandard, Market: "MX"},
		{ProductID: "PRD-005", Name: "Café Molido 250g", Sku: "CAF-250-VAC", Status: domain.StatusActive, TaxCategory: domain.TaxStandard, Market: "MX"},
		{ProductID: "PRD-006", Name: "Leche Entera 1L", Sku: "LEC-1000-BOX", Status: domain.StatusActive, TaxCategory: domain.TaxExempt, Market: "MX"},
		{ProductID: "PRD-007", Name: "Jugo Naranja 1L", Sku: "JUG-NAR-100", Status: domain.StatusActive, TaxCategory: domain.TaxReduced, Market: "MX"},

		// Mercado CO (Colombia)
		{ProductID: "PRD-002", Name: "Agua Mineral 1.5L", Sku: "AGU-1500-PET", Status: domain.StatusActive, TaxCategory: domain.TaxExempt, Market: "CO"},
		{ProductID: "PRD-004", Name: "Pan Integral 500g", Sku: "PAN-INT-500", Status: domain.StatusActive, TaxCategory: domain.TaxReduced, Market: "CO"},
		{ProductID: "PRD-009", Name: "Aceite Oliva 500ml", Sku: "ACE-OLI-500", Status: domain.StatusActive, TaxCategory: domain.TaxStandard, Market: "CO"},

		// Mercado PE (Perú)
		{ProductID: "PRD-003", Name: "Galletas Chocolate 200g", Sku: "GAL-CHO-200", Status: domain.StatusDiscontinued, TaxCategory: domain.TaxStandard, Market: "PE"},
		{ProductID: "PRD-010", Name: "Té Verde 20s", Sku: "TEV-020-BOX", Status: domain.StatusDiscontinued, TaxCategory: domain.TaxExempt, Market: "PE"},
	}

	return &ProductRepository{products: seed}
}

func (r *ProductRepository) GetByIDAndMarket(ctx context.Context, id, market string) (domain.Product, error) {
	for _, p := range r.products {
		// Respetar la cancelación del context si el cliente cancela o caduca el deadline
		select {
		case <-ctx.Done():
			return domain.Product{}, ctx.Err()
		default:
		}

		if p.ProductID == id && p.Market == market {
			return p, nil
		}
	}
	return domain.Product{}, domain.ErrProductNotFound
}
