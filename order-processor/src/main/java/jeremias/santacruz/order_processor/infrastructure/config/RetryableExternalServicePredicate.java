package jeremias.santacruz.order_processor.infrastructure.config;

import jeremias.santacruz.order_processor.domain.port.out.ExternalServiceException;

import java.util.function.Predicate;

/**
 * Predicado de reintento HTTP por excepción para las instancias de {@code Retry} de Resilience4j
 * (propiedad {@code resilience4j.retry.instances.*.retry-exception-predicate}).
 *
 * <p>Solo reintenta los fallos clasificados como transitorios por el dominio
 * ({@link ExternalServiceException#isRetryable()}): {@code 429}/{@code 5xx}/timeout/red y el
 * {@code 429} local del rate limiter. Los errores definitivos (404, respuestas fuera de contrato,
 * resto de códigos) salen sin reintento HTTP para conservar su clasificación en el caso de uso:
 * un reintento no los convierte en procesables.</p>
 *
 * <p>Debe exponer un constructor público sin argumentos: Resilience4j lo instancia por reflexión
 * ({@code ClassUtils.instantiatePredicateClass}).</p>
 */
public class RetryableExternalServicePredicate implements Predicate<Throwable> {

    @Override
    public boolean test(Throwable throwable) {
        return throwable instanceof ExternalServiceException exception && exception.isRetryable();
    }
}