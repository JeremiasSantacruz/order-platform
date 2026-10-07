package jeremias.santacruz.order_processor.infrastructure.adapter.in.kafka;

import jeremias.santacruz.order_processor.domain.port.in.ProcessOrderUseCase;
import jeremias.santacruz.order_processor.domain.port.in.ProcessOrderUseCase.OrderLineCommand;
import jeremias.santacruz.order_processor.domain.port.in.ProcessOrderUseCase.ProcessOrderCommand;
import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Contrato de entrada: el listener decodifica, valida las 7 reglas de la sección 5.A y mapea al
 * comando del dominio antes de invocar el caso de uso. Las violaciones salen como
 * {@link ContractViolationException} (sin reintentos, directo a la DLT).
 */
class OrderCreatedListenerTest {

    private static ValidatorFactory factory;

    private ProcessOrderUseCase useCase;
    private OrderCreatedListener listener;

    @BeforeAll
    static void createValidatorFactory() {
        factory = Validation.buildDefaultValidatorFactory();
    }

    @AfterAll
    static void closeValidatorFactory() {
        factory.close();
    }

    @BeforeEach
    void setUp() {
        useCase = mock(ProcessOrderUseCase.class);
        OrderCreatedEventValidator validator = new OrderCreatedEventValidator(factory.getValidator());
        listener = new OrderCreatedListener(useCase, validator, new OrderEventJsonReader());
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
    @DisplayName("JSON inválido → ContractViolationException (DESERIALIZATION) sin llegar al caso de uso")
    void shouldRejectMalformedJson() {
        assertThatThrownBy(() -> listener.onMessage(record(OrderEventFixtures.malformedPayload())))
                .isInstanceOfSatisfying(ContractViolationException.class,
                        ex -> assertThat(ex.getCategory())
                                .isEqualTo(ContractViolationException.Category.DESERIALIZATION));
        verify(useCase, never()).processOrder(any());
    }

    @Test
    @DisplayName("quantity decimal (regla 4) → ContractViolationException sin llegar al caso de uso")
    void shouldRejectFractionalQuantity() {
        assertThatThrownBy(() -> listener.onMessage(record(OrderEventFixtures.fractionalQuantityPayload())))
                .isInstanceOf(ContractViolationException.class);
        verify(useCase, never()).processOrder(any());
    }

    @Test
    @DisplayName("items vacío (regla 2) → ContractViolationException CONTRACT_VIOLATION")
    void shouldRejectEmptyItems() {
        assertThatThrownBy(() -> listener.onMessage(record(OrderEventFixtures.emptyItemsPayload())))
                .isInstanceOfSatisfying(ContractViolationException.class,
                        ex -> assertThat(ex.getCategory())
                                .isEqualTo(ContractViolationException.Category.CONTRACT_VIOLATION));
        verify(useCase, never()).processOrder(any());
    }

    @Test
    @DisplayName("orderId ausente (regla 1) → ContractViolationException CONTRACT_VIOLATION")
    void shouldRejectMissingOrderId() {
        assertThatThrownBy(() -> listener.onMessage(record(OrderEventFixtures.payloadWithoutOrderId())))
                .isInstanceOfSatisfying(ContractViolationException.class,
                        ex -> assertThat(ex.getCategory())
                                .isEqualTo(ContractViolationException.Category.CONTRACT_VIOLATION));
        verify(useCase, never()).processOrder(any());
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
}
