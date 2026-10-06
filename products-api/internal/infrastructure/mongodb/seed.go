package mongodb

import (
	"context"
	"log"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"products-api/internal/domain"
)

func (r *ProductRepository) EnsureIndexesAndSeed(ctx context.Context) error {
	indexModel := mongo.IndexModel{
		Keys: bson.D{
			{Key: "product_id", Value: 1},
			{Key: "market", Value: 1},
		},
		Options: options.Index().SetUnique(true),
	}

	_, err := r.collection.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return err
	}

	count, err := r.collection.CountDocuments(ctx, bson.M{})
	if err != nil || count > 0 {
		return err // Ya hay datos cargados
	}

	seed := []any{
		productDocument{ProductID: "PRD-001", Name: "Bebida 600 ml", Sku: "BEB-600-PET", Status: string(domain.StatusActive), TaxCategory: string(domain.TaxStandard), Market: "AR"},
		productDocument{ProductID: "PRD-002", Name: "Agua Mineral 1.5L", Sku: "AGU-1500-PET", Status: string(domain.StatusActive), TaxCategory: string(domain.TaxExempt), Market: "AR"},
		productDocument{ProductID: "PRD-003", Name: "Galletas Chocolate 200g", Sku: "GAL-CHO-200", Status: string(domain.StatusDiscontinued), TaxCategory: string(domain.TaxStandard), Market: "AR"},
		productDocument{ProductID: "PRD-004", Name: "Pan Integral 500g", Sku: "PAN-INT-500", Status: string(domain.StatusActive), TaxCategory: string(domain.TaxReduced), Market: "AR"},
		productDocument{ProductID: "PRD-005", Name: "Café Molido 250g", Sku: "CAF-250-VAC", Status: string(domain.StatusActive), TaxCategory: string(domain.TaxStandard), Market: "MX"},
		productDocument{ProductID: "PRD-006", Name: "Leche Entera 1L", Sku: "LEC-1000-BOX", Status: string(domain.StatusActive), TaxCategory: string(domain.TaxExempt), Market: "MX"},
		productDocument{ProductID: "PRD-007", Name: "Jugo Naranja 1L", Sku: "JUG-NAR-100", Status: string(domain.StatusActive), TaxCategory: string(domain.TaxReduced), Market: "MX"},
		productDocument{ProductID: "PRD-008", Name: "Cereal Avena 500g", Sku: "CER-AVE-500", Status: string(domain.StatusActive), TaxCategory: string(domain.TaxStandard), Market: "CL"},
		productDocument{ProductID: "PRD-009", Name: "Aceite Oliva 500ml", Sku: "ACE-OLI-500", Status: string(domain.StatusActive), TaxCategory: string(domain.TaxStandard), Market: "CL"},
		productDocument{ProductID: "PRD-010", Name: "Té Verde 20s", Sku: "TEV-020-BOX", Status: string(domain.StatusDiscontinued), TaxCategory: string(domain.TaxExempt), Market: "CL"},
	}

	_, err = r.collection.InsertMany(ctx, seed)
	if err == nil {
		log.Println("MongoDB: Seed data inserted successfully.")
	}
	return err
}
