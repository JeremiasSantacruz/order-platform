package jeremias.santacruz.order_processor.infrastructure.adapter.out.http;

import jeremias.santacruz.order_processor.domain.model.product.Product;
import jeremias.santacruz.order_processor.domain.model.product.ProductStatus;
import jeremias.santacruz.order_processor.domain.model.product.TaxCategory;
import jeremias.santacruz.order_processor.domain.port.out.ExternalServiceException;
import jeremias.santacruz.order_processor.domain.port.out.ProductsClientPort;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Adaptador de salida hacia la Products API (Go), contrato de la sección 5.C.
 *
 * <p>La consulta por lote recorre el endpoint individual {@code GET /products/{id}?market={market}}
 * (la spec no define endpoint de lote). Un {@code 404} deja el producto fuera del mapa, lo que el
 * caso de uso clasifica como {@code PRODUCT_NOT_FOUND}; cualquier fallo transitorio interrumpe el
 * lote completo para reintentar el mensaje con todo el contexto.</p>
 */
@Component
public class ProductsClientAdapter implements ProductsClientPort {

    private static final String SERVICE = "products-api";

    private final RestClient restClient;

    public ProductsClientAdapter(@Qualifier("productsApiRestClient") RestClient restClient) {
        this.restClient = restClient;
    }

    @Override
    public Map<String, Product> getProducts(Set<String> productIds, String market) {
        Map<String, Product> products = new LinkedHashMap<>();
        for (String productId : productIds) {
            getProduct(productId, market).ifPresent(product -> products.put(productId, product));
        }
        return products;
    }

    @Override
    public Optional<Product> getProduct(String productId, String market) {
        ProductResponse response;
        try {
            response = restClient.get()
                    .uri("/products/{productId}?market={market}", productId, market)
                    .retrieve()
                    .body(ProductResponse.class);
        }
        catch (RestClientResponseException ex) {
            if (ex.getStatusCode().value() == 404) {
                return Optional.empty();
            }
            throw ExternalServiceException.forStatus(SERVICE, ex.getStatusCode().value(), ex.getMessage());
        }
        catch (RestClientException ex) {
            // Timeout o falla de red: siempre transitoria.
            throw ExternalServiceException.networkFailure(SERVICE, ex);
        }

        if (response == null) {
            throw new ExternalServiceException(SERVICE, 200, false,
                    "Respuesta vacía de GET /products/" + productId);
        }
        return Optional.of(toDomain(response, productId));
    }

    /** Mapea la respuesta 200 (5.C) al modelo de dominio; los enums fuera del contrato son definitivos. */
    private Product toDomain(ProductResponse response, String productId) {
        try {
            return new Product(
                    response.productId() != null ? response.productId() : productId,
                    response.name(),
                    response.sku(),
                    ProductStatus.valueOf(response.status()),
                    TaxCategory.valueOf(response.taxCategory()));
        }
        catch (IllegalArgumentException | NullPointerException ex) {
            throw new ExternalServiceException(SERVICE, 200, false,
                    "Respuesta de Products API fuera del contrato: " + ex.getMessage());
        }
    }

    /** Contrato JSON de la sección 5.C. */
    record ProductResponse(String productId, String name, String sku, String status, String taxCategory) {}
}
