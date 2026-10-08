package jeremias.santacruz.order_processor.infrastructure.adapter.out.http;

import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.RequestNotPermitted;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
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
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.client.ExpectedCount.once;
import static org.springframework.test.web.client.ExpectedCount.times;
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
    @DisplayName("RequestNotPermitted del aspecto en un GET → 429 transitorio")
    void shouldTranslateRequestNotPermittedToRetryable429() {
        RateLimiter limiter = RateLimiter.of("products-api", RateLimiterConfig.custom()
                .limitForPeriod(1)
                .limitRefreshPeriod(Duration.ofMinutes(10))
                .timeoutDuration(Duration.ofMillis(50))
                .build());

        assertThatThrownBy(() -> adapter.rateLimitFallback("PRD-001", "MX",
                RequestNotPermitted.createRequestNotPermitted(limiter)))
                .isInstanceOfSatisfying(ExternalServiceException.class, ex -> {
                    assertThat(ex.getStatusCode()).isEqualTo(429);
                    assertThat(ex.isRetryable()).isTrue();
                });
    }

    @Test
    @DisplayName("Otros fallos pasan por el fallback sin alterarse")
    void shouldRethrowNonRateLimitFailures() {
        ExternalServiceException original = ExternalServiceException.forStatus("products-api", 502, "bad gateway");

        assertThatThrownBy(() -> adapter.rateLimitFallback("PRD-001", "MX", original)).isSameAs(original);
    }

    @Test
    @DisplayName("El throttling se declara con @RateLimiter sobre ambos métodos del puerto")
    void shouldBeAnnotatedWithRateLimiter() throws NoSuchMethodException {
        io.github.resilience4j.ratelimiter.annotation.RateLimiter batch =
                ProductsClientAdapter.class.getMethod("getProducts", Set.class, String.class)
                        .getAnnotation(io.github.resilience4j.ratelimiter.annotation.RateLimiter.class);
        io.github.resilience4j.ratelimiter.annotation.RateLimiter single =
                ProductsClientAdapter.class.getMethod("getProduct", String.class, String.class)
                        .getAnnotation(io.github.resilience4j.ratelimiter.annotation.RateLimiter.class);

        assertThat(batch).isNotNull();
        assertThat(batch.name()).isEqualTo("products-api");
        assertThat(batch.fallbackMethod()).isEqualTo("rateLimitFallback");
        assertThat(single).isNotNull();
    }

    @Test
    @DisplayName("El reintento HTTP se declara con @Retry sobre ambos métodos del puerto")
    void shouldBeAnnotatedWithRetry() throws NoSuchMethodException {
        io.github.resilience4j.retry.annotation.Retry batch =
                ProductsClientAdapter.class.getMethod("getProducts", Set.class, String.class)
                        .getAnnotation(io.github.resilience4j.retry.annotation.Retry.class);
        io.github.resilience4j.retry.annotation.Retry single =
                ProductsClientAdapter.class.getMethod("getProduct", String.class, String.class)
                        .getAnnotation(io.github.resilience4j.retry.annotation.Retry.class);

        assertThat(batch).isNotNull();
        assertThat(batch.name()).isEqualTo("products-api");
        assertThat(single).isNotNull();
        assertThat(single.name()).isEqualTo("products-api");
    }

    @Test
    @DisplayName("@Retry: el 503 transitorio del lote se reintenta hasta maxAttempts y luego propaga")
    void shouldRetryTransientFailuresUpToMaxAttempts() {
        server.expect(times(3), requestTo("http://products.test/products/PRD-001?market=MX"))
                .andRespond(withStatus(HttpStatus.SERVICE_UNAVAILABLE));

        Retry retry = retryForTest();
        Set<String> ids = new LinkedHashSet<>(List.of("PRD-001"));

        assertThatThrownBy(() -> Retry.decorateSupplier(retry,
                () -> adapter.getProducts(ids, "MX")).get())
                .isInstanceOfSatisfying(ExternalServiceException.class,
                        ex -> assertThat(ex.isRetryable()).isTrue());
        server.verify();
    }

    @Test
    @DisplayName("@Retry: el 401 definitivo NO se reintenta")
    void shouldNotRetryDefinitiveFailures() {
        server.expect(once(), requestTo("http://products.test/products/PRD-001?market=MX"))
                .andRespond(withStatus(HttpStatus.UNAUTHORIZED));

        Retry retry = retryForTest();
        Set<String> ids = new LinkedHashSet<>(List.of("PRD-001"));

        assertThatThrownBy(() -> Retry.decorateSupplier(retry,
                () -> adapter.getProducts(ids, "MX")).get())
                .isInstanceOfSatisfying(ExternalServiceException.class,
                        ex -> assertThat(ex.isRetryable()).isFalse());
        server.verify();
    }

    /** Simula la instancia {@code resilience4j.retry.instances.products-api.*} para el test. */
    private static Retry retryForTest() {
        return Retry.of("products-api", RetryConfig.custom()
                .maxAttempts(3)
                .retryOnException(ex -> ex instanceof ExternalServiceException e && e.isRetryable())
                .intervalFunction(io.github.resilience4j.core.IntervalFunction.of(Duration.ofMillis(10)))
                .failAfterMaxAttempts(true)
                .build());
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