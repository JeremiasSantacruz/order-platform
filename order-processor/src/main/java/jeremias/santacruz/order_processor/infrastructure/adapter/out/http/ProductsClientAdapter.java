package jeremias.santacruz.order_processor.infrastructure.adapter.out.http;

import io.github.resilience4j.ratelimiter.RequestNotPermitted;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import io.github.resilience4j.retry.annotation.Retry;
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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

/**
 * Adaptador de salida hacia la Products API (Go), contrato de la sección 5.C.
 *
 * <p>La consulta por lote recorre el endpoint individual {@code GET /products/{id}?market={market}}
 * (la spec no define endpoint de lote). Un {@code 404} deja el producto fuera del mapa, lo que el
 * caso de uso clasifica como {@code PRODUCT_NOT_FOUND}; cualquier fallo transitorio interrumpe el
 * lote completo para reintentar el mensaje con todo el contexto.</p>
 *
 * <p>Cada consulta del lote se ejecuta en un <b>hilo virtual</b> propio (JDK 21): el HTTP es
 * bloqueante, así que los hilos se desmontan durante la espera de red y el lote termina en el
 * tiempo de la consulta más lenta en lugar de la suma de todas. Un {@link Semaphore} acota las
 * peticiones en vuelo para no saturar la API con pedidos de muchas líneas. El resultado conserva
 * el orden de entrada y la primera falla (en ese orden) es la que se propaga, de modo que el
 * reintento y la clasificación de errores no dependen de qué respuesta llegó antes.</p>
 *
 * <p>El throttling es declarativo ({@link RateLimiter}, propiedades {@code resilience4j.ratelimiter.instances.products-api.*}).
 * El lote consume un permiso por invocación (el GET interno no pasa por el proxy, igual que antes
 * de paralelizar); el {@code fallbackMethod} traduce el {@link RequestNotPermitted} del aspecto a
 * un {@code 429} local transitorio y re-lanza intacto cualquier otro fallo.</p>
 *
 * <p>El reintento HTTP es declarativo ({@link Retry}, propiedades {@code resilience4j.retry.instances.products-api.*}):
 * se reintentan con backoff exponencial solo los fallos transitorios
 * ({@code RetryableExternalServicePredicate}); los definitivos se propagan intactos.</p>
 */
@Component
public class ProductsClientAdapter implements ProductsClientPort {

    private static final String SERVICE = "products-api";

    /**
     * Cota de peticiones concurrentes por lote. Evita que un pedido con muchas líneas dispare
     * todas las consultas a la vez; los hilos virtuales sobrantes esperan en el semáforo sin
     * ocupar hilos de plataforma.
     */
    static final int MAX_CONCURRENT_REQUESTS = 5;

    private final RestClient restClient;

    public ProductsClientAdapter(@Qualifier("productsApiRestClient") RestClient restClient) {
        this.restClient = restClient;
    }

    @Override
    @Retry(name = "products-api")
    @RateLimiter(name = "products-api", fallbackMethod = "rateLimitFallback")
    public Map<String, Product> getProducts(Set<String> productIds, String market) {
        if (productIds == null || productIds.isEmpty()) {
            return Map.of();
        }
        List<String> ids = List.copyOf(productIds);
        Semaphore inFlight = new Semaphore(MAX_CONCURRENT_REQUESTS);
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Callable<Optional<Product>>> tasks = new ArrayList<>(ids.size());
            for (String productId : ids) {
                tasks.add(() -> {
                    inFlight.acquire();
                    try {
                        return fetchProduct(productId, market);
                    }
                    finally {
                        inFlight.release();
                    }
                });
            }
            // invokeAll espera a todas las consultas (o las cancela si el hilo se interrumpe)
            // y devuelve los futuros en el mismo orden en que se enviaron las tareas.
            List<Future<Optional<Product>>> futures = executor.invokeAll(tasks);
            Map<String, Product> products = new LinkedHashMap<>();
            for (int i = 0; i < ids.size(); i++) {
                String productId = ids.get(i);
                Optional<Product> product = await(futures.get(i), productId);
                product.ifPresent(value -> products.put(productId, value));
            }
            return products;
        }
        catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Consulta de productos interrumpida para market=" + market, ex);
        }
    }

    @Override
    @Retry(name = "products-api")
    @RateLimiter(name = "products-api", fallbackMethod = "rateLimitFallback")
    public Optional<Product> getProduct(String productId, String market) {
        return fetchProduct(productId, market);
    }

    /** Consulta HTTP de un producto. Sin anotaciones: el contexto de resiliencia lo aporta el llamador. */
    private Optional<Product> fetchProduct(String productId, String market) {
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

    /** Espera el resultado de una consulta preservando el tipo de fallo original. */
    private static Optional<Product> await(Future<Optional<Product>> future, String productId) {
        try {
            return future.get();
        }
        catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Consulta interrumpida del producto " + productId, ex);
        }
        catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Consulta interrumpida del producto " + productId, interrupted);
            }
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new IllegalStateException("Fallo inesperado consultando el producto " + productId, cause);
        }
    }

    /** Fallback del lote: {@link RequestNotPermitted} → 429 transitorio; el resto se re-lanza intacto. */
    public Map<String, Product> rateLimitFallback(Set<String> productIds, String market, Throwable ex) {
        rateLimitRethrow(productIds, market, ex);
        throw new IllegalStateException("rethrow incondicional", ex);
    }

    /** Fallback del GET individual: {@link RequestNotPermitted} → 429 transitorio; el resto se re-lanza intacto. */
    public Optional<Product> rateLimitFallback(String productId, String market, Throwable ex) {
        rateLimitRethrow(productId, market, ex);
        throw new IllegalStateException("rethrow incondicional", ex);
    }

    /** El token bucket agotado (tras {@code timeout-duration}) se comporta como un 429 transitorio. */
    private void rateLimitRethrow(Object subject, String market, Throwable ex) {
        if (ex instanceof RequestNotPermitted) {
            throw new ExternalServiceException(SERVICE, 429, true,
                    "Rate limit de " + SERVICE + " alcanzado al invocar GET /products/" + subject
                            + "?market=" + market + ": " + ex.getMessage());
        }
        if (ex instanceof RuntimeException runtime) {
            throw runtime;
        }
        throw new ExternalServiceException(SERVICE, null, true, "Fallo inesperado: " + ex.getMessage());
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
