package mongodb

import "products-api/internal/domain"

type productDocument struct {
	ProductID   string `bson:"product_id"`
	Name        string `bson:"name"`
	Sku         string `bson:"sku"`
	Status      string `bson:"status"`
	TaxCategory string `bson:"tax_category"`
	Market      string `bson:"market"`
}

func (p productDocument) toDomain() domain.Product {
	return domain.Product{
		ProductID:   p.ProductID,
		Name:        p.Name,
		Sku:         p.Sku,
		Status:      domain.Status(p.Status),
		TaxCategory: domain.TaxCategory(p.TaxCategory),
		Market:      p.Market,
	}
}
