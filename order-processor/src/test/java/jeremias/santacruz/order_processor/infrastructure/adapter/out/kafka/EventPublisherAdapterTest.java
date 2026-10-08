package jeremias.santacruz.order_processor.infrastructure.adapter.out.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jeremias.santacruz.order_processor.domain.model.order.Currency;
import jeremias.santacruz.order_processor.domain.model.order.Market;
import jeremias.santacruz.order_processor.domain.model.order.Order;
import jeremias.santacruz.order_processor.domain.model.order.OrderLine;
import jeremias.santacruz.order_processor.domain.model.order.OrderTotals;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * El adaptador de salida publica el resultado en {@code orders.processed.v1} con el contrato de
 * la sección 5.D intacto: los campos del evento no cambian y un pedido parcial puede llevar
 * {@code market}/{@code currency} en {@code null} en el JSON.
 */
class EventPublisherAdapterTest {

    private static final String EVENT_ID = "01J8ZP6M5E4RH0K7Y2N9A3TQWX";

    private KafkaTemplate<String, String> kafkaTemplate;
    private ObjectMapper objectMapper;
    private EventPublisherAdapter adapter;

    @BeforeEach
    void setUp() {
        kafkaTemplate = mock(KafkaTemplate.class);
        objectMapper = new ObjectMapper();
        adapter = new EventPublisherAdapter(kafkaTemplate, objectMapper, "orders.processed.v1");
    }

    private Order approvedOrder() {
        Order order = new Order("ORD-MX-000147", EVENT_ID, 1L, Market.MX, Currency.MXN, "CLI-99821",
                List.of(new OrderLine("PRD-001", 24, new BigDecimal("35.5"))),
                Instant.parse("2026-09-18T15:42:10Z"));
        order.approve(new OrderTotals(new BigDecimal("852.00"), new BigDecimal("25.56"),
                new BigDecimal("826.44"), new BigDecimal("132.23"), new BigDecimal("958.67")),
                List.of(), null, Instant.parse("2026-09-18T15:42:11Z"));
        return order;
    }

    private void stubSend() {
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
    }

    private JsonNode publishedPayload() throws Exception {
        ArgumentCaptor<String> payload = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq("orders.processed.v1"), eq("ORD-MX-000147"), payload.capture());
        return objectMapper.readTree(payload.getValue());
    }

    @Test
    @DisplayName("Publica orders.processed.v1 con el contrato exacto de la sección 5.D")
    void shouldPublishOutputEventWithSpecContract() throws Exception {
        stubSend();

        adapter.publishOrderProcessed(approvedOrder());

        JsonNode json = publishedPayload();
        assertThat(json.get("eventId").asText()).isEqualTo(EVENT_ID + "-OUT");
        assertThat(json.get("eventVersion").asInt()).isEqualTo(1);
        assertThat(json.get("sourceEventId").asText()).isEqualTo(EVENT_ID);
        assertThat(json.get("orderId").asText()).isEqualTo("ORD-MX-000147");
        assertThat(json.get("status").asText()).isEqualTo("APPROVED");
        assertThat(json.get("market").asText()).isEqualTo("MX");
        assertThat(json.get("currency").asText()).isEqualTo("MXN");
        assertThat(json.get("occurredAt").asText()).isNotBlank();
        assertThat(json.get("totals").get("grossSubtotal").decimalValue()).isEqualByComparingTo("852.00");
        assertThat(json.get("totals").get("discount").decimalValue()).isEqualByComparingTo("25.56");
        assertThat(json.get("totals").get("netSubtotal").decimalValue()).isEqualByComparingTo("826.44");
        assertThat(json.get("totals").get("tax").decimalValue()).isEqualByComparingTo("132.23");
        assertThat(json.get("totals").get("grandTotal").decimalValue()).isEqualByComparingTo("958.67");
        assertThat(json.get("reason").isNull()).isTrue();
    }

    @Test
    @DisplayName("Un pedido con totales nulos (fallo técnico) publica totales en cero")
    void shouldPublishZeroTotalsWhenTotalsAreNull() throws Exception {
        stubSend();
        Order failure = new Order("ORD-MX-000147", EVENT_ID, 1L, Market.MX, Currency.MXN, "CLI-99821",
                List.of(new OrderLine("PRD-001", 1, BigDecimal.ONE)), null);
        failure.markTechnicalFailure("TECHNICAL_FAILURE: products-api", Instant.now());

        adapter.publishOrderProcessed(failure);

        JsonNode json = publishedPayload();
        assertThat(json.get("status").asText()).isEqualTo("TECHNICAL_FAILURE");
        assertThat(json.get("totals").get("grandTotal").decimalValue()).isEqualByComparingTo("0");
        assertThat(json.get("reason").asText()).contains("TECHNICAL_FAILURE");
    }

    @Test
    @DisplayName("Un pedido parcial (market/currency desconocidos) publica null en el JSON sin romper el contrato")
    void shouldPublishNullMarketWhenMarketIsUnknown() throws Exception {
        stubSend();
        String orderId = "UNPARSEABLE-orders.created.v1-0-7";
        Order partial = Order.partial(orderId, "evt-9", 0L, null, null, null, List.of(), Instant.now());
        partial.reject("DESERIALIZATION: JSON inválido", Instant.now());

        adapter.publishOrderProcessed(partial);

        ArgumentCaptor<String> payload = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq("orders.processed.v1"), eq(orderId), payload.capture());
        JsonNode json = objectMapper.readTree(payload.getValue());
        assertThat(json.get("status").asText()).isEqualTo("REJECTED");
        assertThat(json.get("market").isNull()).isTrue();
        assertThat(json.get("currency").isNull()).isTrue();
        assertThat(json.get("reason").asText()).contains("DESERIALIZATION");
    }
}