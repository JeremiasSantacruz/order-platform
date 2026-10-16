package jeremias.santacruz.order_processor.infrastructure.adapter.in.kafka;

import jeremias.santacruz.order_processor.domain.port.in.ProcessOrderUseCase;
import jeremias.santacruz.order_processor.domain.port.in.ProcessOrderUseCase.OrderLineCommand;
import jeremias.santacruz.order_processor.domain.port.in.ProcessOrderUseCase.ProcessOrderCommand;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Contrato de entrada: el listener decodifica, valida las 7 reglas de la sección 5.A y mapea al
 * comando del dominio. Un payload no parseable o fuera de contrato es un fallo no recuperable:
 * se lanza {@link ContractViolationException} para que el manejador de errores lo derive a la DLT.
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

    private ContractViolationException contractViolation(String payload) {
        return (ContractViolationException) org.assertj.core.api.Assertions
                .catchThrowable(() -> listener.onMessage(record(payload)));
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
    @DisplayName("JSON inválido → ContractViolationException(DESERIALIZATION); no llega al caso de uso")
    void shouldThrowDeserializationViolationForMalformedJson() {
        ContractViolationException violation = contractViolation(OrderEventFixtures.malformedPayload());

        assertThat(violation.getCategory()).isEqualTo(ContractViolationException.Category.DESERIALIZATION);
        assertThat(violation.getSummaryCause()).contains("payload no interpretable");
        verify(useCase, never()).processOrder(any());
    }

    @Test
    @DisplayName("Regla 2 (items vacío) → ContractViolationException(CONTRACT_VIOLATION) directo a DLT")
    void shouldThrowContractViolationForEmptyItems() {
        ContractViolationException violation = contractViolation(OrderEventFixtures.emptyItemsPayload());

        assertThat(violation.getCategory()).isEqualTo(ContractViolationException.Category.CONTRACT_VIOLATION);
        assertThat(violation.getSummaryCause()).contains("items debe tener al menos 1 elemento");
        verify(useCase, never()).processOrder(any());
    }

    @Test
    @DisplayName("Regla 1 (falta orderId) → ContractViolationException(CONTRACT_VIOLATION)")
    void shouldThrowContractViolationWhenOrderIdMissing() {
        ContractViolationException violation = contractViolation(OrderEventFixtures.payloadWithoutOrderId());

        assertThat(violation.getCategory()).isEqualTo(ContractViolationException.Category.CONTRACT_VIOLATION);
        assertThat(violation.getSummaryCause()).contains("orderId es obligatorio");
    }

    @Test
    @DisplayName("quantity decimal (regla 4) no deserializa en int → DESERIALIZATION sin llegar al caso de uso")
    void shouldThrowDeserializationForFractionalQuantity() {
        ContractViolationException violation =
                contractViolation(OrderEventFixtures.fractionalQuantityPayload());

        assertThat(violation.getCategory()).isEqualTo(ContractViolationException.Category.DESERIALIZATION);
        verify(useCase, never()).processOrder(any());
    }
}
