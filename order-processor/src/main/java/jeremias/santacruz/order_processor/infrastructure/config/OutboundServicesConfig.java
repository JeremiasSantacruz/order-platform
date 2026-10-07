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
 * falla de red <b>transitoria</b> y dispara el reintento con backoff del contenedor de Kafka.</p>
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
