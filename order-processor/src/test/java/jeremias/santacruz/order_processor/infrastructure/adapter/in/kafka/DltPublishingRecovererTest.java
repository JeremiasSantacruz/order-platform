package jeremias.santacruz.order_processor.infrastructure.adapter.in.kafka;

import jeremias.santacruz.order_processor.domain.port.out.EventPublisherPort;
import jeremias.santacruz.order_processor.domain.port.out.EventPublisherPort.DeadLetterInfo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.support.KafkaHeaders;

import java.nio.ByteBuffer;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * El recoverer traduce el fallo a los metadatos de la sección 7 y delega la publicación en la
 * DLT al único camino de salida ({@link EventPublisherPort#publishToDLT}).
 */
class DltPublishingRecovererTest {

    private static final String PAYLOAD = """
            {"eventId":"01J8ZP6M5E4RH0K7Y2N9A3TQWX","orderId":"ORD-MX-000147","market":"MX"}
            """;

    private EventPublisherPort eventPublisherPort;
    private DltPublishingRecoverer recoverer;

    @BeforeEach
    void setUp() {
        eventPublisherPort = mock(EventPublisherPort.class);
        recoverer = new DltPublishingRecoverer(eventPublisherPort, new OrderEventJsonReader(),
                "order-processor", 3);
    }

    private static ConsumerRecord<String, String> record(String payload, String key) {
        return new ConsumerRecord<>("orders.created.v1", 0, 0L, key, payload);
    }

    private DeadLetterInfo capturedInfo() {
        ArgumentCaptor<DeadLetterInfo> captor = ArgumentCaptor.forClass(DeadLetterInfo.class);
        verify(eventPublisherPort).publishToDLT(eq(PAYLOAD), captor.capture());
        return captor.getValue();
    }

    @Test
    @DisplayName("Violación de contrato: errorCategory del dominio, attemptCount 1 y payload original")
    void shouldMapContractViolationMetadata() {
        Exception wrapped = new ListenerExecutionFailedException("mensaje inválido",
                new ContractViolationException(ContractViolationException.Category.CONTRACT_VIOLATION,
                        "items debe tener al menos 1 elemento"));

        recoverer.accept(record(PAYLOAD, "ORD-MX-000147"), wrapped);

        DeadLetterInfo info = capturedInfo();
        assertThat(info.orderId()).isEqualTo("ORD-MX-000147");
        assertThat(info.eventId()).isEqualTo("01J8ZP6M5E4RH0K7Y2N9A3TQWX");
        assertThat(info.errorCategory()).isEqualTo("CONTRACT_VIOLATION");
        assertThat(info.summaryCause()).isEqualTo("items debe tener al menos 1 elemento");
        assertThat(info.attemptCount()).isEqualTo(1);
        assertThat(info.timestamp()).isNotNull();
        assertThat(info.component()).isEqualTo("order-processor");
    }

    @Test
    @DisplayName("Fallo transitorio agotado: errorCategory RETRIES_EXHAUSTED y causa raíz")
    void shouldMapExhaustedRetriesMetadata() {
        Exception wrapped = new ListenerExecutionFailedException("fallo",
                new IllegalStateException("causada", new RuntimeException("429 rate limit")));

        recoverer.accept(record(PAYLOAD, "ORD-MX-000147"), wrapped);

        DeadLetterInfo info = capturedInfo();
        assertThat(info.errorCategory()).isEqualTo("RETRIES_EXHAUSTED");
        assertThat(info.summaryCause()).contains("RuntimeException: 429 rate limit");
        // Sin cabecera de intento: se usa max-retries + 1 (fallback).
        assertThat(info.attemptCount()).isEqualTo(4);
    }

    @Test
    @DisplayName("Lee attemptCount real de la cabecera de entrega del contenedor")
    void shouldReadDeliveryAttemptHeader() {
        ConsumerRecord<String, String> record = record(PAYLOAD, "ORD-MX-000147");
        record.headers().add(KafkaHeaders.DELIVERY_ATTEMPT, ByteBuffer.allocate(4).putInt(3).array());
        Exception wrapped = new ListenerExecutionFailedException("fallo", new RuntimeException("timeout"));

        recoverer.accept(record, wrapped);

        assertThat(capturedInfo().attemptCount()).isEqualTo(3);
    }

    @Test
    @DisplayName("Payload ilegible: conserva orderId de la clave y marca eventId como unknown")
    void shouldFallBackToRecordKeyWhenPayloadIsUnreadable() {
        String brokenPayload = "no-json";
        ArgumentCaptor<DeadLetterInfo> captor = ArgumentCaptor.forClass(DeadLetterInfo.class);

        recoverer.accept(record(brokenPayload, "ORD-CLAVE"),
                new ListenerExecutionFailedException("fallo", new RuntimeException("boom")));

        verify(eventPublisherPort).publishToDLT(eq(brokenPayload), captor.capture());
        DeadLetterInfo info = captor.getValue();
        assertThat(info.orderId()).isEqualTo("ORD-CLAVE");
        assertThat(info.eventId()).isEqualTo("unknown");
        assertThat(info.errorCategory()).isEqualTo("RETRIES_EXHAUSTED");
    }

    @Test
    @DisplayName("Nunca publica sin pasar por el puerto de salida (un solo camino de DLT)")
    void shouldAlwaysDelegateThroughPublisherPort() {
        recoverer.accept(record(PAYLOAD, null),
                new ListenerExecutionFailedException("fallo", new RuntimeException("boom")));

        verify(eventPublisherPort).publishToDLT(eq(PAYLOAD), any(DeadLetterInfo.class));
    }

    @Test
    @DisplayName("La clave del registro no se pierde cuando orderId existe en el payload")
    void shouldPreferOrderIdFromPayload() {
        recoverer.accept(record(PAYLOAD, "clave-antigua"),
                new ListenerExecutionFailedException("fallo", new RuntimeException("boom")));

        assertThat(capturedInfo().orderId()).isEqualTo("ORD-MX-000147");
    }
}
