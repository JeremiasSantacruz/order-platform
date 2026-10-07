package jeremias.santacruz.order_processor.domain.port.out;

import java.util.Set;

/**
 * Fallo técnico al consultar un servicio externo (Clients API, Products API, etc.).
 *
 * <p>Clasifica el error según la matriz de la sección 7 de Especificaciones.md:</p>
 * <ul>
 *   <li>{@code 429}, {@code 500}, {@code 502}, {@code 503} y fallos de red/timeout son
 *       <b>transitorios</b> ({@link #isRetryable()} == {@code true}) y admiten reintento con
 *       backoff.</li>
 *   <li>El {@code 404} no se reporta con esta excepción: los puertos lo traducen a
 *       {@code Optional.empty()}; el resto de códigos se consideran <b>definitivos</b>.</li>
 * </ul>
 *
 * <p>Es parte del contrato del dominio: no depende de Spring ni de ningún framework HTTP.</p>
 */
public class ExternalServiceException extends RuntimeException {

    private static final Set<Integer> TRANSIENT_STATUS_CODES = Set.of(429, 500, 502, 503);

    private final String service;
    private final Integer statusCode;
    private final boolean retryable;

    /**
     * @param service   Nombre del servicio externo (p. ej. {@code clients-api})
     * @param statusCode Código HTTP de la respuesta, o {@code null} si no hubo respuesta (timeout/red)
     * @param retryable  {@code true} si el error es transitorio y corresponde reintentar
     * @param message    Detalle del fallo
     */
    public ExternalServiceException(String service, Integer statusCode, boolean retryable, String message) {
        super(message);
        this.service = service;
        this.statusCode = statusCode;
        this.retryable = retryable;
    }

    /**
     * Clasifica un código HTTP según la matriz de errores del spec.
     */
    public static ExternalServiceException forStatus(String service, int statusCode, String message) {
        return new ExternalServiceException(service, statusCode, TRANSIENT_STATUS_CODES.contains(statusCode), message);
    }

    /**
     * Falla de red o timeout durante la llamada; siempre transitoria.
     */
    public static ExternalServiceException networkFailure(String service, Throwable cause) {
        return new ExternalServiceException(service, null, true,
                "Falla de red/timeout al invocar " + service + ": " + cause.getMessage());
    }

    public String getService() {
        return service;
    }

    public Integer getStatusCode() {
        return statusCode;
    }

    /** {@code true} si el error es transitorio (429, 5xx, timeout) y el worker debe reintentar. */
    public boolean isRetryable() {
        return retryable;
    }
}