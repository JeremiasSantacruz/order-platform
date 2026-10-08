package jeremias.santacruz.order_processor.infrastructure.adapter.out.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jeremias.santacruz.order_processor.domain.model.order.Order;
import jeremias.santacruz.order_processor.domain.model.order.OrderTotals;
import jeremias.santacruz.order_processor.domain.port.out.EventPublisherPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Adaptador de salida Kafka: publica el resultado del pedido en {@code orders.processed.v1}
 * (sección 5.D). Es el único destino de salida: no existe DLT, todo resultado se publica aquí.
 *
 * <p>Las publicaciones son <b>bloqueantes</b>: si Kafka no acepta el mensaje, la excepción escala
 * al contenedor, el offset no se confirma y la re-entrega vuelve a intentarlo (la ruta idempotente
 * del caso de uso reenvía el resultado ya persistido). Es la semántica at-least-once elegida para
 * la consistencia Mongo ↔ Kafka (ver ADR-002).</p>
 */
@Component
public class EventPublisherAdapter implements EventPublisherPort {

    private static final Logger log = LoggerFactory.getLogger(EventPublisherAdapter.class);

    /** Versión del contrato del tópico de salida {@code orders.processed.v1}. */
    private static final long OUTPUT_EVENT_VERSION = 1L;

    private static final long SEND_TIMEOUT_SECONDS = 10L;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String processedTopic;

    public EventPublisherAdapter(KafkaTemplate<String, String> kafkaTemplate,
                                 ObjectMapper objectMapper,
                                 @Value("${app.kafka.topics.orders-processed:orders.processed.v1}") String processedTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.processedTopic = processedTopic;
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

    /**
     * Mapeo del agregado de dominio al contrato de salida (5.D).
     *
     * <p>El contrato no cambia: un pedido rechazado por payload ilegible o por contrato puede
     * venir con {@code market}/{@code currency} desconocidos, y esos campos se publican en
     * {@code null} en lugar de romper la serialización.</p>
     */
    private OrderProcessedEventDto toEvent(Order order) {
        OrderTotals totals = order.getTotals() != null ? order.getTotals() : OrderTotals.zero();
        return new OrderProcessedEventDto(
                order.getEventId() + "-OUT",
                OUTPUT_EVENT_VERSION,
                Instant.now().toString(),
                order.getEventId(),
                order.getOrderId(),
                order.getStatus().name(),
                order.getMarket() == null ? null : order.getMarket().name(),
                order.getCurrency() == null ? null : order.getCurrency().name(),
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
}