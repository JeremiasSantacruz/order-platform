package jeremias.santacruz.order_processor.infrastructure.adapter.out.http;

import io.github.resilience4j.ratelimiter.RequestNotPermitted;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import io.github.resilience4j.retry.annotation.Retry;
import jeremias.santacruz.order_processor.domain.model.client.Client;
import jeremias.santacruz.order_processor.domain.model.client.ClientSegment;
import jeremias.santacruz.order_processor.domain.model.client.ClientStatus;
import jeremias.santacruz.order_processor.domain.model.client.TaxRegime;
import jeremias.santacruz.order_processor.domain.model.order.Market;
import jeremias.santacruz.order_processor.domain.port.out.ClientsClientPort;
import jeremias.santacruz.order_processor.domain.port.out.ExternalServiceException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

import java.util.Optional;

/**
 * Adaptador de salida hacia la Clients API (NestJS), contrato de la sección 5.B.
 *
 * <p>Clasificación de errores según la matriz de la sección 7:</p>
 * <ul>
 *   <li>{@code 404} → {@code Optional.empty()} (el caso de uso lo traduce a {@code REJECTED}).</li>
 *   <li>{@code 429}/{@code 5xx}/timeout/red → {@link ExternalServiceException} reintentable
 *       (el contenedor Kafka reintenta con backoff y, al agotar, registra {@code TECHNICAL_FAILURE}).</li>
 *   <li>Resto de códigos y respuestas fuera del contrato → error no reintentable
 *       (estado {@code TECHNICAL_FAILURE}).</li>
 * </ul>
 *
 * <p>El throttling es declarativo ({@link RateLimiter}, propiedades {@code resilience4j.ratelimiter.instances.clients-api.*}).
 * El aspecto de Resilience4j rechaza la llamada con {@link RequestNotPermitted} antes de entrar al método; el
 * {@code fallbackMethod} traduce ese rechazo a un {@code 429} local transitorio y re-lanza intacto cualquier
 * otro fallo (el cuerpo ya los mapea a {@link ExternalServiceException}).</p>
 *
 * <p>El reintento HTTP es declarativo ({@link Retry}, propiedades {@code resilience4j.retry.instances.clients-api.*}):
 * se reintentan con backoff exponencial solo los fallos transitorios
 * ({@code RetryableExternalServicePredicate}); los definitivos se propagan intactos.</p>
 */
@Component
public class ClientsClientAdapter implements ClientsClientPort {

    private static final String SERVICE = "clients-api";

    private final RestClient restClient;

    public ClientsClientAdapter(@Qualifier("clientsApiRestClient") RestClient restClient) {
        this.restClient = restClient;
    }

    @Override
    @Retry(name = "clients-api")
    @RateLimiter(name = "clients-api", fallbackMethod = "rateLimitFallback")
    public Optional<Client> getClient(String clientId) {
        ClientResponse response;
        try {
            response = restClient.get()
                    .uri("/clients/{clientId}", clientId)
                    .retrieve()
                    .body(ClientResponse.class);
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
                    "Respuesta vacía de GET /clients/" + clientId);
        }
        return Optional.of(toDomain(response, clientId));
    }

    /**
     * Fallback del {@link RateLimiter}: el token bucket agotado (tras {@code timeout-duration}) se
     * comporta como un 429 transitorio; el resto de fallos se re-lanzan sin alterarlos.
     */
    public Optional<Client> rateLimitFallback(String clientId, Throwable ex) {
        if (ex instanceof RequestNotPermitted) {
            throw new ExternalServiceException(SERVICE, 429, true,
                    "Rate limit de " + SERVICE + " alcanzado al invocar GET /clients/" + clientId
                            + ": " + ex.getMessage());
        }
        if (ex instanceof RuntimeException runtime) {
            throw runtime;
        }
        throw new ExternalServiceException(SERVICE, null, true, "Fallo inesperado: " + ex.getMessage());
    }

    /** Mapea la respuesta 200 (5.B) al modelo de dominio; los enums fuera del contrato son definitivos. */
    private Client toDomain(ClientResponse response, String clientId) {
        try {
            return new Client(
                    response.clientId() != null ? response.clientId() : clientId,
                    response.name(),
                    ClientStatus.valueOf(response.status()),
                    ClientSegment.valueOf(response.segment()),
                    TaxRegime.valueOf(response.taxRegime()),
                    Market.valueOf(response.market()));
        }
        catch (IllegalArgumentException | NullPointerException ex) {
            throw new ExternalServiceException(SERVICE, 200, false,
                    "Respuesta de Clients API fuera del contrato: " + ex.getMessage());
        }
    }

    /** Contrato JSON de la sección 5.B. */
    record ClientResponse(String clientId, String name, String status, String segment,
                          String taxRegime, String market) {}
}