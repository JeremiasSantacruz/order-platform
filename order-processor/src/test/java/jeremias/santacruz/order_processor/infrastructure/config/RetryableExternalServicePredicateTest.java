package jeremias.santacruz.order_processor.infrastructure.config;

import jeremias.santacruz.order_processor.domain.port.out.ExternalServiceException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * El predicado decide qué excepciones son susceptibles de reintento HTTP: solo los fallos
 * transitorios (429/5xx/timeout/red y el 429 local del rate limiter) según la matriz de la sección 7.
 */
class RetryableExternalServicePredicateTest {

    private final RetryableExternalServicePredicate predicate = new RetryableExternalServicePredicate();

    @Test
    @DisplayName("Reintenta solo fallos transitorios de servicios externos")
    void retriesTransientFailures() {
        assertThat(predicate.test(ExternalServiceException.forStatus("clients-api", 429, "too many"))).isTrue();
        assertThat(predicate.test(ExternalServiceException.forStatus("clients-api", 503, "upstream down"))).isTrue();
        assertThat(predicate.test(ExternalServiceException.networkFailure("clients-api",
                new RuntimeException("read timed out")))).isTrue();
        assertThat(predicate.test(new ExternalServiceException("clients-api", 429, true,
                "local rate limit"))).isTrue();
    }

    @Test
    @DisplayName("No reintenta fallos definitivos ni otras excepciones")
    void doesNotRetryDefinitiveFailures() {
        assertThat(predicate.test(ExternalServiceException.forStatus("clients-api", 401, "unauthorized"))).isFalse();
        assertThat(predicate.test(new ExternalServiceException("clients-api", 200, false,
                "respuesta fuera de contrato"))).isFalse();
        assertThat(predicate.test(new IllegalStateException("bug interno"))).isFalse();
        assertThat(predicate.test(null)).isFalse();
    }
}