package jeremias.santacruz.order_processor.infrastructure.config;

import jeremias.santacruz.order_processor.infrastructure.adapter.in.kafka.ContractViolationException;
import jeremias.santacruz.order_processor.infrastructure.adapter.in.kafka.DltPublishingRecoverer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.MessageListenerContainer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Política de errores del consumidor (sección 7): violaciones de contrato van directo a la DLT
 * sin reintentos; los fallos transitorios esperan el backoff exponencial.
 */
class KafkaConsumerConfigTest {

    private final DltPublishingRecoverer recoverer = mock(DltPublishingRecoverer.class);
    private final DefaultErrorHandler handler = new KafkaConsumerConfig().ordersCreatedErrorHandler(recoverer, 3);
    private final ConsumerRecord<String, String> record =
            new ConsumerRecord<>("orders.created.v1", 0, 0L, "ORD-1", "{}");
    private final Consumer<?, ?> consumer = mock(Consumer.class);
    private final MessageListenerContainer container = mock(MessageListenerContainer.class);

    KafkaConsumerConfigTest() {
        when(container.getContainerProperties()).thenReturn(new ContainerProperties("orders.created.v1"));
    }

    @Test
    @DisplayName("Violación de contrato: se recupera inmediatamente, sin reintentos (directo a DLT)")
    void shouldRecoverContractViolationWithoutRetries() {
        Exception violation = new ListenerExecutionFailedException("mensaje inválido",
                new ContractViolationException(ContractViolationException.Category.CONTRACT_VIOLATION,
                        "items debe tener al menos 1 elemento"));

        handler.handleOne(violation, record, consumer, container);

        verify(recoverer).accept(eq(record), any(Exception.class));
    }

    @Test
    @DisplayName("Fallo transitorio: NO se recupera en el primer intento (queda para el backoff)")
    void shouldNotRecoverTransientFailureOnFirstAttempt() {
        Exception transientFailure = new ListenerExecutionFailedException("fallo transitorio",
                new RuntimeException("429 rate limit"));

        handler.handleOne(transientFailure, record, consumer, container);

        verify(recoverer, never()).accept(any(), any());
    }

    @Test
    @DisplayName("Backoff exponencial: tras agotar los reintentos configurados, va al recoverer (DLT)")
    void shouldRecoverOnlyAfterConfiguredRetriesAreExhausted() {
        Exception transientFailure = new ListenerExecutionFailedException("fallo transitorio",
                new RuntimeException("timeout"));

        // Los primeros intentos esperan el backoff sin publicar en la DLT.
        for (int i = 0; i < 3; i++) {
            handler.handleOne(transientFailure, record, consumer, container);
        }
        verify(recoverer, never()).accept(any(), any());

        // El siguiente fallo agota ExponentialBackOffWithMaxRetries(3) → recuperación (DLT).
        handler.handleOne(transientFailure, record, consumer, container);
        verify(recoverer).accept(eq(record), any(Exception.class));
    }
}
