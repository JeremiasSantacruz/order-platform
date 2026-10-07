package jeremias.santacruz.order_processor.infrastructure.adapter.out.http;

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
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
