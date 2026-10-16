package jeremias.santacruz.order_processor.infrastructure.adapter.in.kafka;

import jeremias.santacruz.order_processor.domain.port.out.EventPublisherPort;
import jeremias.santacruz.order_processor.domain.port.out.EventPublisherPort.DeadLetterInfo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.time.Instant;

/**
 * Clasifica los mensajes que no pudieron procesarse y los deriva a la DLT
 * {@code orders.processing.dlt} (sección 7 de Especificaciones.md).
 *
 * <p>Su responsabilidad es traducir el fallo a los metadatos {@link DeadLetterInfo}
 * ({@code orderId}, {@code eventId}, {@code errorCategory}, {@code summaryCause},
 * {@code attemptCount}, {@code timestamp} y {@code component}); la construcción del registro y su
 * publicación viven en el adaptador de salida {@link EventPublisherPort#publishToDLT}, que es el
 * único camino de DLT de la aplicación.</p>
 *
 * <p>El payload original se reenvía sin modificar para que el análisis posterior del incidente
 * no dependa de reproducir el evento.</p>
 */
@Component
public class DltPublishingRecoverer implements ConsumerRecordRecoverer {

    private static final Logger log = LoggerFactory.getLogger(DltPublishingRecoverer.class);

    /** Categoría para fallos técnicos agotados (no son violaciones de contrato). */
    static final String RETRIES_EXHAUSTED = "RETRIES_EXHAUSTED";

    private static final String UNKNOWN = "unknown";

    private final EventPublisherPort eventPublisherPort;
    private final OrderEventJsonReader jsonReader;
    private final String component;
    private final int fallbackAttemptCount;

    public DltPublishingRecoverer(EventPublisherPort eventPublisherPort,
                                  OrderEventJsonReader jsonReader,
                                  @Value("${spring.application.name:order-processor}") String component,
                                  @Value("${app.kafka.consumer.max-retries:3}") int maxRetries) {
        this.eventPublisherPort = eventPublisherPort;
        this.jsonReader = jsonReader;
        this.component = component;
        this.fallbackAttemptCount = maxRetries + 1;
    }

    @Override
    public void accept(ConsumerRecord<?, ?> record, Exception exception) {
        ContractViolationException violation = ContractViolationException.findIn(exception);
        String payload = record.value() == null ? null : record.value().toString();

        DeadLetterInfo info = new DeadLetterInfo(
                jsonReader.readTextField(payload, "orderId").orElse(keyOf(record)),
                jsonReader.readTextField(payload, "eventId").orElse(UNKNOWN),
                violation != null ? violation.getCategory().name() : RETRIES_EXHAUSTED,
                violation != null ? violation.getSummaryCause() : describe(exception),
                attemptCountOf(record, violation),
                Instant.now(),
                component);

        log.warn("Mensaje eventId={} orderId={} descartado (errorCategory={}); se envía a la DLT",
                info.eventId(), info.orderId(), info.errorCategory());
        eventPublisherPort.publishToDLT(payload, info);
    }

    /**
     * Intentos de entrega del mensaje.
     *
     * <p>La cabecera {@link KafkaHeaders#DELIVERY_ATTEMPT} la añade el contenedor cuando está
     * habilitada (ver {@code KafkaConsumerConfig}) y refleja el intento real. Para violaciones de
     * contrato no hay reintento: falla en el primer intento.</p>
     */
    private int attemptCountOf(ConsumerRecord<?, ?> record, ContractViolationException violation) {
        if (violation != null) {
            return 1;
        }
        Header attemptHeader = record.headers().lastHeader(KafkaHeaders.DELIVERY_ATTEMPT);
        if (attemptHeader != null && attemptHeader.value().length == Integer.BYTES) {
            return ByteBuffer.wrap(attemptHeader.value()).getInt();
        }
        return fallbackAttemptCount;
    }

    private static String keyOf(ConsumerRecord<?, ?> record) {
        return record.key() == null ? null : record.key().toString();
    }

    private static String describe(Throwable throwable) {
        Throwable root = throwable;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String message = root.getMessage();
        return root.getClass().getSimpleName() + (message == null || message.isBlank() ? "" : ": " + message);
    }
}
