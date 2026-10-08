package domain

import "context"

type Status string
type TaxCategory string
type Market string

const (
	StatusActive       Status = "ACTIVE"
	StatusDiscontinued Status = "DISCONTINUED"

	TaxStandard TaxCategory = "STANDARD"
	TaxReduced  TaxCategory = "REDUCED"
	TaxExempt   TaxCategory = "EXEMPT"

	MarketMX Market = "MX"
	MarketCO Market = "CO"
	MarketPE Market = "PE"
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
