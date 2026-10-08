package jeremias.santacruz.order_processor.infrastructure.adapter.in.kafka;

import jeremias.santacruz.order_processor.domain.port.in.ProcessOrderUseCase;
import jeremias.santacruz.order_processor.domain.port.in.ProcessOrderUseCase.ProcessOrderCommand;
import jeremias.santacruz.order_processor.domain.port.in.ProcessOrderUseCase.UnprocessableCommand;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.ListenerExecutionFailedException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Cuando se agotan los reintentos, {@link ProcessingFailureRecoverer} registra el resultado
 * definitivo del mensaje con el caso de uso: {@code TECHNICAL_FAILURE} si el payload sigue siendo
 * parseable, o no procesable si no pudo leerse. No hay DLT.
 */
class ProcessingFailureRecovererTest {

    private static final String PAYLOAD = """
            {"eventId":"01J8ZP6M5E4RH0K7Y2N9A3TQWX","orderId":"ORD-MX-000147","market":"MX",\
            "currency":"MXN","clientId":"CLI-99821","channel":"C1","eventVersion":1,\
            "items":[{"productId":"PRD-001","quantity":1,"unitPrice":10.0}]}
            """;

    private ProcessOrderUseCase useCase;
    private ProcessingFailureRecoverer recoverer;

    @BeforeEach
    void setUp() {
        useCase = mock(ProcessOrderUseCase.class);
        recoverer = new ProcessingFailureRecoverer(useCase, new OrderEventJsonReader());
    }

    private static ConsumerRecord<String, String> record(String payload, String key) {
        return new ConsumerRecord<>("orders.created.v1", 0, 0L, key, payload);
    }

    private ProcessOrderCommand capturedCommand() {
        org.mockito.ArgumentCaptor<ProcessOrderCommand> captor =
                org.mockito.ArgumentCaptor.forClass(ProcessOrderCommand.class);
        verify(useCase).recordTechnicalFailure(captor.capture(), anyString());
        return captor.getValue();
    }

    @Test
    @DisplayName("Payload parseable con reintentos agotados → recordTechnicalFailure mapeando el payload")
    void shouldRecordTechnicalFailureForParseablePayload() {
        Exception wrapped = new ListenerExecutionFailedException("fallo",
                new IllegalStateException("causada", new RuntimeException("429 rate limit")));

        recoverer.accept(record(PAYLOAD, "ORD-MX-000147"), wrapped);

        ProcessOrderCommand command = capturedCommand();
        assertThat(command.orderId()).isEqualTo("ORD-MX-000147");
        assertThat(command.eventId()).isEqualTo("01J8ZP6M5E4RH0K7Y2N9A3TQWX");
    }

    @Test
    @DisplayName("Payload ilegible con reintentos agotados → recordUnprocessable con salvage e id sintético")
    void shouldRecordUnprocessableForUnreadablePayload() {
        String brokenPayload = "no-json";

        recoverer.accept(record(brokenPayload, "ORD-CLAVE"),
                new ListenerExecutionFailedException("fallo", new RuntimeException("boom")));

        org.mockito.ArgumentCaptor<UnprocessableCommand> captor =
                org.mockito.ArgumentCaptor.forClass(UnprocessableCommand.class);
        verify(useCase).recordUnprocessable(captor.capture());
        verify(useCase, never()).recordTechnicalFailure(any(), anyString());

        UnprocessableCommand command = captor.getValue();
        assertThat(command.orderId()).isEqualTo("ORD-CLAVE");
        assertThat(command.eventId()).isEqualTo("orders.created.v1-0-0");
        assertThat(command.reason()).startsWith("RETRIES_EXHAUSTED");
        assertThat(command.sourcePayload()).isEqualTo(brokenPayload);
    }

    @Test
    @DisplayName("La causa raíz del fallo va en la razón técnica")
    void shouldDescribeRootCauseInReason() {
        recoverer.accept(record(PAYLOAD, "ORD-MX-000147"),
                new ListenerExecutionFailedException("fallo",
                        new RuntimeException("causada", new RuntimeException("429 rate limit"))));

        org.mockito.ArgumentCaptor<String> captor = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(useCase).recordTechnicalFailure(any(), captor.capture());
        assertThat(captor.getValue()).contains("RuntimeException: 429 rate limit");
    }
}