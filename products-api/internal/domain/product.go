package domain

import "context"

type Status string
type TaxCategory string

const (
	StatusActive       Status = "ACTIVE"
	StatusDiscontinued Status = "DISCONTINUED"

	TaxStandard TaxCategory = "STANDARD"
	TaxReduced  TaxCategory = "REDUCED"
	TaxExempt   TaxCategory = "EXEMPT"
)

type Product struct {
	ProductID   string      `json:"productId"`
	Name        string      `json:"name"`
	Sku         string      `json:"sku"`
	Status      Status      `json:"status"`
	TaxCategory TaxCategory `json:"taxCategory"`
	Market      string      `json:"-"` 
}

type ProductRepository interface {
	GetByIDAndMarket(ctx context.Context, id, market string) (Product, error)
}
