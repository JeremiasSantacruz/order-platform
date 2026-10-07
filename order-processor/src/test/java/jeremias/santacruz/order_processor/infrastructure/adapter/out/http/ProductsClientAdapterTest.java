package jeremias.santacruz.order_processor.infrastructure.adapter.out.http;

import jeremias.santacruz.order_processor.domain.model.product.Product;
import jeremias.santacruz.order_processor.domain.model.product.ProductStatus;
import jeremias.santacruz.order_processor.domain.model.product.TaxCategory;
import jeremias.santacruz.order_processor.domain.port.out.ExternalServiceException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

/**
 * Contrato 5.C y matriz de errores de la sección 7 aplicada al adaptador de Products API.
 */
class ProductsClientAdapterTest {

    private static final String PRODUCT_JSON = """
            {"productId":"PRD-001","name":"Bebida 600 ml","sku":"BEB-600-PET",
             "status":"ACTIVE","taxCategory":"STANDARD"}
            """;

    private MockRestServiceServer server;
    private ProductsClientAdapter adapter;

    @BeforeEach
    void setUp() {
        RestClient.Builder builder = RestClient.builder().baseUrl("http://products.test");
        server = MockRestServiceServer.bindTo(builder).build();
        adapter = new ProductsClientAdapter(builder.build());
    }

    @Test
    @DisplayName("Mapea la respuesta 200 del contrato 5.C al modelo de dominio")
    void shouldMapProductResponse() {
        server.expect(requestTo("http://products.test/products/PRD-001?market=MX"))
                .andRespond(withSuccess(PRODUCT_JSON, MediaType.APPLICATION_JSON));

        Optional<Product> product = adapter.getProduct("PRD-001", "MX");

        assertThat(product).contains(
                new Product("PRD-001", "Bebida 600 ml", "BEB-600-PET", ProductStatus.ACTIVE, TaxCategory.STANDARD));
    }

    @Test
    @DisplayName("Lote: el 404 de un producto lo deja fuera del mapa (PRODUCT_NOT_FOUND)")
    void shouldExcludeMissingProductsFromBatch() {
        server.expect(requestTo("http://products.test/products/PRD-001?market=MX"))
                .andRespond(withSuccess(PRODUCT_JSON, MediaType.APPLICATION_JSON));
        server.expect(requestTo("http://products.test/products/PRD-404?market=MX"))
                .andRespond(withStatus(HttpStatus.NOT_FOUND));

        Set<String> ids = new LinkedHashSet<>(List.of("PRD-001", "PRD-404"));
        Map<String, Product> products = adapter.getProducts(ids, "MX");

        assertThat(products).containsOnlyKeys("PRD-001");
        assertThat(products.get("PRD-001").taxCategory()).isEqualTo(TaxCategory.STANDARD);
    }

    @Test
    @DisplayName("429 en cualquier ítem del lote → transitorio y se interrumpe el lote")
    void shouldPropagateRetryableFromBatch() {
        server.expect(requestTo("http://products.test/products/PRD-001?market=MX"))
                .andRespond(withStatus(HttpStatus.TOO_MANY_REQUESTS));

        Set<String> ids = new LinkedHashSet<>(List.of("PRD-001", "PRD-008"));
        assertThatThrownBy(() -> adapter.getProducts(ids, "MX"))
                .isInstanceOfSatisfying(ExternalServiceException.class,
                        ex -> assertThat(ex.isRetryable()).isTrue());
    }

    @Test
    @DisplayName("Timeout/falla de red → transitorio (sección 7)")
    void shouldThrowRetryableOnTimeout() {
        server.expect(requestTo("http://products.test/products/PRD-001?market=MX"))
                .andRespond(request -> {
                    throw new IOException("read timed out");
                });

        assertThatThrownBy(() -> adapter.getProduct("PRD-001", "MX"))
                .isInstanceOfSatisfying(ExternalServiceException.class,
                        ex -> assertThat(ex.isRetryable()).isTrue());
    }
}
