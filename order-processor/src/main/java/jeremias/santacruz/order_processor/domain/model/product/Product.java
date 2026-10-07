package jeremias.santacruz.order_processor.domain.model.product;

public record Product(
        String productId,
        String name,
        String sku,
        ProductStatus status,
        TaxCategory taxCategory
) {
public boolean isActive() {
    return status == ProductStatus.ACTIVE;
}
}

