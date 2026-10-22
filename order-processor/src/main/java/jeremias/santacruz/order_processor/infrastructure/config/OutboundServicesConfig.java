package jeremias.santacruz.order_processor.infrastructure.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.net.http.HttpClient;
import java.time.Duration;

/**
 * Fábrica de los clientes HTTP de salida (Clients API y Products API).
 *
 * <p>Los timeouts son parte de la matriz de la sección 7: un read timeout se clasifica como
 * falla de red <b>transitoria</b>.</p>
 *
 * <p>El transporte es el {@link HttpClient} del JDK (HTTP/1.1), no {@code HttpURLConnection}:
 * es la implementación recomendada cuando las llamadas bloqueantes corren sobre hilos
 * virtuales, porque no usa {@code synchronized} para coordinar el I/O y no expone al hilo
 * portador a quedar <i>pinned</i>.</p>
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

    /**
     * Request factory sobre {@link HttpClient}. El timeout de conexión se configura en el
     * cliente y el de lectura en la factory, manteniendo las mismas propiedades que antes.
     */
    private static JdkClientHttpRequestFactory requestFactory(long connectTimeoutMs, long readTimeoutMs) {
        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(connectTimeoutMs))
                // HTTP/1.1 explícito: misma semántica de transporte que HttpURLConnection.
                .version(HttpClient.Version.HTTP_1_1)
                // HttpURLConnection sigue redirecciones en GET; se conserva ese comportamiento.
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
        JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(httpClient);
        factory.setReadTimeout(Duration.ofMillis(readTimeoutMs));
        return factory;
    }
}
