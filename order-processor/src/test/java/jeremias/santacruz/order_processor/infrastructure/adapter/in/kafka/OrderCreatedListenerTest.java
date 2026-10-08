package jeremias.santacruz.order_processor.infrastructure.adapter.in.kafka;

import jeremias.santacruz.order_processor.domain.port.in.ProcessOrderUseCase;
import jeremias.santacruz.order_processor.domain.port.in.ProcessOrderUseCase.OrderLineCommand;
import jeremias.santacruz.order_processor.domain.port.in.ProcessOrderUseCase.ProcessOrderCommand;
import jeremias.santacruz.order_processor.domain.port.in.ProcessOrderUseCase.UnprocessableCommand;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Contrato de entrada: el listener decodifica y mapea al comando del dominio; las reglas del
 * contrato de la sección 5.A se evalúan dentro del caso de uso, así que un payload que es JSON
 * válido siempre llega a {@code processOrder}. Un payload no parseable no sale del flujo: se
 * registra como pedido no procesable con {@code recordUnprocessable}.
 */
class OrderCreatedListenerTest {

    private ProcessOrderUseCase useCase;
    private OrderCreatedListener listener;

    @BeforeEach
    void setUp() {
        useCase = mock(ProcessOrderUseCase.class);
        listener = new OrderCreatedListener(useCase, new OrderEventJsonReader());
    }

    private static ConsumerRecord<String, String> record(String payload) {
        return new ConsumerRecord<>("orders.created.v1", 0, 0L, "ORD-MX-000147", payload);
    }

    @Test
    @DisplayName("Payload válido: mapea al comando del dominio e invoca el caso de uso")
    void shouldMapValidPayloadToCommand() {
        listener.onMessage(record(OrderEventFixtures.validPayload()));

        ArgumentCaptor<ProcessOrderCommand> captor = ArgumentCaptor.forClass(ProcessOrderCommand.class);
        verify(useCase).processOrder(captor.capture());

        ProcessOrderCommand command = captor.getValue();
        assertThat(command.eventId()).isEqualTo("01J8ZP6M5E4RH0K7Y2N9A3TQWX");
        assertThat(command.eventVersion()).isEqualTo(1L);
        assertThat(command.orderId()).isEqualTo("ORD-MX-000147");
        assertThat(command.market()).isEqualTo("MX");
        assertThat(command.currency()).isEqualTo("MXN");
        assertThat(command.clientId()).isEqualTo("CLI-99821");
        assertThat(command.channel()).isEqualTo("C1");
        assertThat(command.items()).hasSize(2);
        OrderLineCommand first = command.items().get(0);
        assertThat(first.productId()).isEqualTo("PRD-001");
        assertThat(first.quantity()).isEqualTo(24);
        assertThat(first.unitPrice()).isEqualByComparingTo("35.5");
    }

    @Test
    @DisplayName("Sin eventVersion se asume la versión 1")
    void shouldDefaultEventVersionToOne() {
        listener.onMessage(record(OrderEventFixtures.payloadWithoutEventVersion()));

        ArgumentCaptor<ProcessOrderCommand> captor = ArgumentCaptor.forClass(ProcessOrderCommand.class);
        verify(useCase).processOrder(captor.capture());
        assertThat(captor.getValue().eventVersion()).isEqualTo(1L);
    }

    @Test
    @DisplayName("El BigDecimal del contrato llega intacto al comando (sin pasar por double)")
    void shouldPreserveBigDecimalPrecision() {
        listener.onMessage(record(OrderEventFixtures.validPayload()));

        ArgumentCaptor<ProcessOrderCommand> captor = ArgumentCaptor.forClass(ProcessOrderCommand.class);
        verify(useCase).processOrder(captor.capture());
        assertThat(captor.getValue().items().get(1).unitPrice())
                .isEqualByComparingTo(new BigDecimal("82.0"))
                .isEqualTo(new BigDecimal("82.0"));
    }

    @Test
    @DisplayName("JSON inválido → recordUnprocessable con los ids salvables y el payload crudo")
    void shouldRecordUnprocessableForMalformedJson() {
        String payload = OrderEventFixtures.malformedPayload();

        listener.onMessage(record(payload));

        ArgumentCaptor<UnprocessableCommand> captor = ArgumentCaptor.forClass(UnprocessableCommand.class);
        verify(useCase).recordUnprocessable(captor.capture());
        verify(useCase, never()).processOrder(org.mockito.ArgumentMatchers.any());

        UnprocessableCommand command = captor.getValue();
        assertThat(command.orderId()).isEqualTo("ORD-MX-000147");
        assertThat(command.eventId()).isEqualTo("orders.created.v1-0-0");
        assertThat(command.reason()).startsWith("DESERIALIZATION");
        assertThat(command.sourcePayload()).isEqualTo(payload);
    }

    @Test
    @DisplayName("JSON inválido sin clave → orderId sintético por tópico/partición/offset")
    void shouldUseSyntheticOrderIdWithoutKey() {
        ConsumerRecord<String, String> record =
                new ConsumerRecord<>("orders.created.v1", 3, 41L, null, "no-json");

        listener.onMessage(record);

        ArgumentCaptor<UnprocessableCommand> captor = ArgumentCaptor.forClass(UnprocessableCommand.class);
        verify(useCase).recordUnprocessable(captor.capture());
        assertThat(captor.getValue().orderId()).isEqualTo("orders.created.v1-3-41");
        assertThat(captor.getValue().eventId()).isEqualTo("orders.created.v1-3-41");
    }

    @Test
    @DisplayName("JSON válido fuera de contrato (regla 2): llega al caso de uso; quantity decimal no deserializa")
    void shouldForwardContractViolationsToUseCase() {
        listener.onMessage(record(OrderEventFixtures.emptyItemsPayload()));

        org.mockito.ArgumentCaptor<ProcessOrderCommand> captor =
                org.mockito.ArgumentCaptor.forClass(ProcessOrderCommand.class);
        verify(useCase).processOrder(captor.capture());
        assertThat(captor.getValue().items()).isEmpty();
    }

    @Test
    @DisplayName("quantity decimal (regla 4) no deserializa en int → recordUnprocessable sin llegar al caso de uso")
    void shouldRecordUnprocessableForFractionalQuantity() {
        listener.onMessage(record(OrderEventFixtures.fractionalQuantityPayload()));

        verify(useCase, never()).processOrder(org.mockito.ArgumentMatchers.any());
        org.mockito.ArgumentCaptor<UnprocessableCommand> captor =
                org.mockito.ArgumentCaptor.forClass(UnprocessableCommand.class);
        verify(useCase).recordUnprocessable(captor.capture());
        assertThat(captor.getValue().reason()).startsWith("DESERIALIZATION");
        assertThat(captor.getValue().orderId()).isEqualTo("ORD-MX-000147");
    }
}