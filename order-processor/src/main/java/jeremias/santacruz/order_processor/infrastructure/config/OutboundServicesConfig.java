package jeremias.santacruz.order_processor.infrastructure.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.time.Duration;

/**
 * Fábrica de los clientes HTTP de salida (Clients API y Products API).
 *
 * <p>Los timeouts son parte de la matriz de la sección 7: un read timeout se clasifica como
 * falla de red <b>transitoria</b>.</p>
 *
 * <p>La resiliencia de las llamadas es declarativa sobre los adaptadores:
 * {@code @Retry} (Resilience4j, propiedades {@code resilience4j.retry.instances.*}) reintenta con
 * backoff exponencial los fallos transitorios antes de propagarlos al contenedor de Kafka, y
 * {@code @RateLimiter} (propiedades {@code resilience4j.ratelimiter.instances.*}) aplica throttling:
 * un {@code 429} local transitorio cuando se agota la espera por un permiso.</p>
 */
@Configuration
public class OutboundServicesConfig {

    @Bean
    RestClient clientsApiRestClient(@Value("${app.clients.base-url}") String baseUrl,
                                    @Value("${app.clients.connect-timeout-ms:3000}") long connectTimeoutMs,
                                    @Value("${app.clients.read-timeout-ms:5000}") long readTimeoutMs) {
        return RestClient.builder()
                .baseUrl(baseUrl)
                .requestFactory(requestFactory(connectTimeoutMs, readTimeoutMs))
                .build();
    }

    @Bean
    RestClient productsApiRestClient(@Value("${app.products.base-url}") String baseUrl,
                                     @Value("${app.products.connect-timeout-ms:3000}") long connectTimeoutMs,
                                     @Value("${app.products.read-timeout-ms:5000}") long readTimeoutMs) {
        return RestClient.builder()
                .baseUrl(baseUrl)
                .requestFactory(requestFactory(connectTimeoutMs, readTimeoutMs))
                .build();
    }

    private static SimpleClientHttpRequestFactory requestFactory(long connectTimeoutMs, long readTimeoutMs) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(Duration.ofMillis(connectTimeoutMs));
        factory.setReadTimeout(Duration.ofMillis(readTimeoutMs));
        return factory;
    }
}