package jeremias.santacruz.order_processor.infrastructure.config;

import jeremias.santacruz.order_processor.infrastructure.adapter.in.kafka.ProcessingFailureRecoverer;
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
 * Política de errores del consumidor: todo fallo (transitorio o no) espera el backoff exponencial
 * y solo al agotar los reintentos {@link ProcessingFailureRecoverer} registra el estado técnico
 * definitivo persistido y publicado. No hay DLT ni casos sin reintentos.
 */
class KafkaConsumerConfigTest {

    private final ProcessingFailureRecoverer recoverer = mock(ProcessingFailureRecoverer.class);
    private final DefaultErrorHandler handler = new KafkaConsumerConfig().ordersCreatedErrorHandler(recoverer, 3);
    private final ConsumerRecord<String, String> record =
            new ConsumerRecord<>("orders.created.v1", 0, 0L, "ORD-1", "{}");
    private final Consumer<?, ?> consumer = mock(Consumer.class);
    private final MessageListenerContainer container = mock(MessageListenerContainer.class);

    KafkaConsumerConfigTest() {
        when(container.getContainerProperties()).thenReturn(new ContainerProperties("orders.created.v1"));
    }

    @Test
    @DisplayName("Fallo: NO se recupera en el primer intento (queda para el backoff)")
    void shouldNotRecoverTransientFailureOnFirstAttempt() {
        Exception transientFailure = new ListenerExecutionFailedException("fallo transitorio",
                new RuntimeException("429 rate limit"));

        handler.handleOne(transientFailure, record, consumer, container);

        verify(recoverer, never()).accept(any(), any());
    }

    @Test
    @DisplayName("Backoff exponencial: tras agotar los reintentos configurados, va al recoverer")
    void shouldRecoverOnlyAfterConfiguredRetriesAreExhausted() {
        Exception transientFailure = new ListenerExecutionFailedException("fallo transitorio",
                new RuntimeException("timeout"));

        // Los primeros intentos esperan el backoff sin registrar nada.
        for (int i = 0; i < 3; i++) {
            handler.handleOne(transientFailure, record, consumer, container);
        }
        verify(recoverer, never()).accept(any(), any());

        // El siguiente fallo agota ExponentialBackOffWithMaxRetries(3) → recuperación.
        handler.handleOne(transientFailure, record, consumer, container);
        verify(recoverer).accept(eq(record), any(Exception.class));
    }

    @Test
    @DisplayName("max-retries negativo se rechaza en la configuración")
    void shouldRejectNegativeMaxRetries() {
        org.assertj.core.api.Assertions.assertThatThrownBy(
                        () -> new KafkaConsumerConfig().ordersCreatedErrorHandler(recoverer, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}