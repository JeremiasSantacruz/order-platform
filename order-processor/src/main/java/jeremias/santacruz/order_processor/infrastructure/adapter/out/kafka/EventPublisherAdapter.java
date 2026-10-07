package jeremias.santacruz.order_processor.infrastructure.adapter.out.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jeremias.santacruz.order_processor.domain.model.order.Order;
import jeremias.santacruz.order_processor.domain.model.order.OrderTotals;
import jeremias.santacruz.order_processor.domain.port.out.EventPublisherPort;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Adaptador de salida Kafka: publica el resultado del pedido en {@code orders.processed.v1}
 * (sección 5.D) y los mensajes fallidos en la DLT {@code orders.processing.dlt} con los headers
 * de metadatos de la sección 7.
 *
 * <p>Las publicaciones de resultado son <b>bloqueantes</b>: si Kafka no acepta el mensaje, la
 * excepción escala al contenedor, el offset no se confirma y la re-entrega vuelve a intentarlo
 * (la ruta idempotente del caso de uso reenvía el resultado ya persistido). Es la semántica
 * at-least-once elegida para la consistencia Mongo ↔ Kafka (ver ADR-002).</p>
 *
 * <p>La publicación en la DLT sí se traga y se loguea: fallar dentro del recoverer causaría un
 * bucle de reentrega con el broker caído.</p>
 */
@Component
public class EventPublisherAdapter implements EventPublisherPort {

    private static final Logger log = LoggerFactory.getLogger(EventPublisherAdapter.class);

    /** Headers de metadatos exigidos por la sección 7 para la DLT. */
    static final String HEADER_ORDER_ID = "orderId";
    static final String HEADER_EVENT_ID = "eventId";
    static final String HEADER_ERROR_CATEGORY = "errorCategory";
    static final String HEADER_SUMMARY_CAUSE = "summaryCause";
    static final String HEADER_ATTEMPT_COUNT = "attemptCount";
    static final String HEADER_TIMESTAMP = "timestamp";
    static final String HEADER_COMPONENT = "component";

    /** Versión del contrato del tópico de salida {@code orders.processed.v1}. */
    private static final long OUTPUT_EVENT_VERSION = 1L;

    private static final long SEND_TIMEOUT_SECONDS = 10L;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String processedTopic;
    private final String dltTopic;
    private final String component;

    public EventPublisherAdapter(KafkaTemplate<String, String> kafkaTemplate,
                                 ObjectMapper objectMapper,
                                 @Value("${app.kafka.topics.orders-processed:orders.processed.v1}") String processedTopic,
                                 @Value("${app.kafka.topics.orders-created-dlt:orders.processing.dlt}") String dltTopic,
                                 @Value("${spring.application.name:order-processor}") String component) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.processedTopic = processedTopic;
        this.dltTopic = dltTopic;
        this.component = component;
    }

    @Override
    public void publishOrderProcessed(Order order) {
        String payload;
        try {
            payload = objectMapper.writeValueAsString(toEvent(order));
        }
        catch (JsonProcessingException ex) {
            throw new IllegalStateException(
                    "No fue posible serializar orders.processed.v1 para el pedido " + order.getOrderId(), ex);
        }
        try {
            send(processedTopic, order.getOrderId(), payload);
            log.info("Pedido {} publicado en '{}' con status={}", order.getOrderId(), processedTopic,
                    order.getStatus());
        }
        catch (RuntimeException ex) {
            log.error("Fallo al publicar el pedido {} en '{}': {}", order.getOrderId(), processedTopic,
                    ex.getMessage());
            throw ex;
        }
    }

    @Override
    public void publishToDLT(String rawPayload, DeadLetterInfo info) {
        ProducerRecord<String, String> record = new ProducerRecord<>(dltTopic, info.orderId(), rawPayload);
        Headers headers = record.headers();
        addHeader(headers, HEADER_ORDER_ID, info.orderId());
        addHeader(headers, HEADER_EVENT_ID, info.eventId());
        addHeader(headers, HEADER_ERROR_CATEGORY, info.errorCategory());
        addHeader(headers, HEADER_SUMMARY_CAUSE, truncate(info.summaryCause()));
        addHeader(headers, HEADER_ATTEMPT_COUNT, Integer.toString(info.attemptCount()));
        addHeader(headers, HEADER_TIMESTAMP, info.timestamp().toString());
        addHeader(headers, HEADER_COMPONENT, info.component());

        try {
            kafkaTemplate.send(record).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.warn("Mensaje eventId={} orderId={} enviado a la DLT '{}' (errorCategory={}, attemptCount={})",
                    info.eventId(), info.orderId(), dltTopic, info.errorCategory(), info.attemptCount());
        }
        catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.error("Interrupción publicando en la DLT '{}': {}", dltTopic, ex.getMessage());
        }
        catch (ExecutionException | TimeoutException ex) {
            log.error("No fue posible publicar en la DLT '{}' el eventId={} orderId={}: {}", dltTopic,
                    info.eventId(), info.orderId(), ex.getMessage());
        }
    }

    /** Mapeo del agregado de dominio al contrato de salida (5.D). */
    private OrderProcessedEventDto toEvent(Order order) {
        OrderTotals totals = order.getTotals() != null ? order.getTotals() : OrderTotals.zero();
        return new OrderProcessedEventDto(
                order.getEventId() + "-OUT",
                OUTPUT_EVENT_VERSION,
                Instant.now().toString(),
                order.getEventId(),
                order.getOrderId(),
                order.getStatus().name(),
                order.getMarket().name(),
                order.getCurrency().name(),
                new OrderProcessedEventDto.TotalsDto(
                        totals.grossSubtotal(), totals.discount(), totals.netSubtotal(),
                        totals.tax(), totals.grandTotal()),
                order.getRejectionReason());
    }

    /** Publicación síncrona: una falla debe impedir el commit del offset. */
    private void send(String topic, String key, String payload) {
        try {
            kafkaTemplate.send(topic, key, payload).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
        catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupción al publicar en '" + topic + "'", ex);
        }
        catch (ExecutionException ex) {
            throw new IllegalStateException("Fallo al publicar en '" + topic + "'", ex.getCause());
        }
        catch (TimeoutException ex) {
            throw new IllegalStateException("Timeout al publicar en '" + topic + "'", ex);
        }
    }

    private static void addHeader(Headers headers, String name, String value) {
        if (value == null) {
            value = "";
        }
        headers.add(new RecordHeader(name, value.getBytes(StandardCharsets.UTF_8)));
    }

    private static String truncate(String value) {
        if (value == null) {
            return "";
        }
        return value.length() <= 500 ? value : value.substring(0, 500);
    }
}
