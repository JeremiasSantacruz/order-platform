package jeremias.santacruz.order_processor.infrastructure.adapter.out.http;

import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.RequestNotPermitted;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import jeremias.santacruz.order_processor.domain.model.client.Client;
import jeremias.santacruz.order_processor.domain.model.client.ClientSegment;
import jeremias.santacruz.order_processor.domain.model.client.ClientStatus;
import jeremias.santacruz.order_processor.domain.model.client.TaxRegime;
import jeremias.santacruz.order_processor.domain.model.order.Market;
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
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.client.ExpectedCount.once;
import static org.springframework.test.web.client.ExpectedCount.times;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

/**
 * Contrato 5.B y matriz de errores de la sección 7 aplicada al adaptador de Clients API.
 */
class ClientsClientAdapterTest {

    private static final String URL = "http://clients.test/clients/CLI-99821";

    private static final String ACTIVE_CLIENT_JSON = """
            {"clientId":"CLI-99821","name":"Distribuidora Central","status":"ACTIVE",
             "segment":"WHOLESALE","taxRegime":"GENERAL","market":"MX"}
            """;

    private MockRestServiceServer server;
    private ClientsClientAdapter adapter;

    @BeforeEach
    void setUp() {
        RestClient.Builder builder = RestClient.builder().baseUrl("http://clients.test");
        server = MockRestServiceServer.bindTo(builder).build();
        adapter = new ClientsClientAdapter(builder.build());
    }

    @Test
    @DisplayName("Mapea la respuesta 200 del contrato 5.B al modelo de dominio")
    void shouldMapClientResponse() {
        server.expect(requestTo(URL)).andRespond(withSuccess(ACTIVE_CLIENT_JSON, MediaType.APPLICATION_JSON));

        Optional<Client> client = adapter.getClient("CLI-99821");

        assertThat(client).contains(new Client("CLI-99821", "Distribuidora Central", ClientStatus.ACTIVE,
                ClientSegment.WHOLESALE, TaxRegime.GENERAL, Market.MX));
    }

    @Test
    @DisplayName("404 → Optional.empty (definitivo: el caso de uso resuelve REJECTED)")
    void shouldReturnEmptyOn404() {
        server.expect(requestTo(URL)).andRespond(withStatus(HttpStatus.NOT_FOUND));

        assertThat(adapter.getClient("CLI-99821")).isEmpty();
    }

    @Test
    @DisplayName("RequestNotPermitted del aspecto → ExternalServiceException 429 reintentable")
    void shouldTranslateRequestNotPermittedToRetryable429() {
        RateLimiter limiter = RateLimiter.of("clients-api", RateLimiterConfig.custom()
                .limitForPeriod(1)
                .limitRefreshPeriod(Duration.ofMinutes(10))
                .timeoutDuration(Duration.ofMillis(50))
                .build());

        assertThatThrownBy(() -> adapter.rateLimitFallback("CLI-99821",
                RequestNotPermitted.createRequestNotPermitted(limiter)))
                .isInstanceOfSatisfying(ExternalServiceException.class, ex -> {
                    assertThat(ex.getStatusCode()).isEqualTo(429);
                    assertThat(ex.isRetryable()).isTrue();
                });
    }

    @Test
    @DisplayName("Otros fallos pasan por el fallback sin alterarse")
    void shouldRethrowNonRateLimitFailures() {
        ExternalServiceException original = ExternalServiceException.forStatus("clients-api", 503, "upstream down");

        assertThatThrownBy(() -> adapter.rateLimitFallback("CLI-99821", original)).isSameAs(original);
    }

    @Test
    @DisplayName("El throttling se declara con @RateLimiter sobre el método del puerto")
    void shouldBeAnnotatedWithRateLimiter() throws NoSuchMethodException {
        io.github.resilience4j.ratelimiter.annotation.RateLimiter annotation =
                ClientsClientAdapter.class.getMethod("getClient", String.class)
                        .getAnnotation(io.github.resilience4j.ratelimiter.annotation.RateLimiter.class);

        assertThat(annotation).isNotNull();
        assertThat(annotation.name()).isEqualTo("clients-api");
        assertThat(annotation.fallbackMethod()).isEqualTo("rateLimitFallback");
    }

    @Test
    @DisplayName("El reintento HTTP se declara con @Retry sobre el método del puerto")
    void shouldBeAnnotatedWithRetry() throws NoSuchMethodException {
        io.github.resilience4j.retry.annotation.Retry annotation =
                ClientsClientAdapter.class.getMethod("getClient", String.class)
                        .getAnnotation(io.github.resilience4j.retry.annotation.Retry.class);

        assertThat(annotation).isNotNull();
        assertThat(annotation.name()).isEqualTo("clients-api");
    }

    @Test
    @DisplayName("@Retry: el 503 transitorio se reintenta hasta maxAttempts y luego propaga")
    void shouldRetryTransientFailuresUpToMaxAttempts() {
        server.expect(times(3), requestTo(URL)).andRespond(withStatus(HttpStatus.SERVICE_UNAVAILABLE));

        Retry retry = retryForTest();

        assertThatThrownBy(() -> Retry.decorateSupplier(retry, () -> adapter.getClient("CLI-99821")).get())
                .isInstanceOfSatisfying(ExternalServiceException.class,
                        ex -> assertThat(ex.isRetryable()).isTrue());
        server.verify();
    }

    @Test
    @DisplayName("@Retry: el 401 definitivo NO se reintenta")
    void shouldNotRetryDefinitiveFailures() {
        server.expect(once(), requestTo(URL)).andRespond(withStatus(HttpStatus.UNAUTHORIZED));

        Retry retry = retryForTest();

        assertThatThrownBy(() -> Retry.decorateSupplier(retry, () -> adapter.getClient("CLI-99821")).get())
                .isInstanceOfSatisfying(ExternalServiceException.class,
                        ex -> assertThat(ex.isRetryable()).isFalse());
        server.verify();
    }

    /** Simula la instancia {@code resilience4j.retry.instances.clients-api.*} para el test. */
    private static Retry retryForTest() {
        return Retry.of("clients-api", RetryConfig.custom()
                .maxAttempts(3)
                .retryOnException(ex -> ex instanceof ExternalServiceException e && e.isRetryable())
                .intervalFunction(io.github.resilience4j.core.IntervalFunction.of(Duration.ofMillis(10)))
                .failAfterMaxAttempts(true)
                .build());
    }

    @Test
    @DisplayName("503 → ExternalServiceException reintentable")
    void shouldThrowRetryableOn503() {
        server.expect(requestTo(URL)).andRespond(withStatus(HttpStatus.SERVICE_UNAVAILABLE));

        assertThatThrownBy(() -> adapter.getClient("CLI-99821"))
                .isInstanceOfSatisfying(ExternalServiceException.class,
                        ex -> assertThat(ex.isRetryable()).isTrue());
    }

    @Test
    @DisplayName("401 → ExternalServiceException definitivo (no reintentable)")
    void shouldThrowDefinitiveOn401() {
        server.expect(requestTo(URL)).andRespond(withStatus(HttpStatus.UNAUTHORIZED));

        assertThatThrownBy(() -> adapter.getClient("CLI-99821"))
                .isInstanceOfSatisfying(ExternalServiceException.class,
                        ex -> assertThat(ex.isRetryable()).isFalse());
    }

    @Test
    @DisplayName("Falla de red/timeout → ExternalServiceException transitorio")
    void shouldThrowRetryableOnNetworkFailure() {
        server.expect(requestTo(URL)).andRespond(request -> {
            throw new IOException("conexion rechazada");
        });

        assertThatThrownBy(() -> adapter.getClient("CLI-99821"))
                .isInstanceOfSatisfying(ExternalServiceException.class, ex -> {
                    assertThat(ex.isRetryable()).isTrue();
                    assertThat(ex.getStatusCode()).isNull();
                });
    }

    @Test
    @DisplayName("Enums fuera del contrato → error definitivo (respuesta 200 no confiable)")
    void shouldThrowDefinitiveOnUnknownEnumValue() {
        server.expect(requestTo(URL)).andRespond(withSuccess(
                "{\"clientId\":\"CLI-99821\",\"name\":\"X\",\"status\":\"FROZEN\"," +
                        "\"segment\":\"WHOLESALE\",\"taxRegime\":\"GENERAL\",\"market\":\"MX\"}",
                MediaType.APPLICATION_JSON));

        assertThatThrownBy(() -> adapter.getClient("CLI-99821"))
                .isInstanceOfSatisfying(ExternalServiceException.class,
                        ex -> assertThat(ex.isRetryable()).isFalse());
    }
}