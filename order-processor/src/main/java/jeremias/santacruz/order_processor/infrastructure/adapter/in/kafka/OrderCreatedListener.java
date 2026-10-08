package jeremias.santacruz.order_processor.infrastructure.adapter.in.kafka;

import jeremias.santacruz.order_processor.domain.port.in.ProcessOrderUseCase;
import jeremias.santacruz.order_processor.domain.port.in.ProcessOrderUseCase.OrderLineCommand;
import jeremias.santacruz.order_processor.domain.port.in.ProcessOrderUseCase.ProcessOrderCommand;
import jeremias.santacruz.order_processor.domain.port.in.ProcessOrderUseCase.UnprocessableCommand;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Adaptador de entrada: consume el evento {@code orders.created.v1} y lo traduce a un comando
 * del dominio ({@link ProcessOrderUseCase}).
 *
 * <p>Responsabilidades, en orden:</p>
 * <ol>
 *   <li>Deserializar el payload crudo; si no es parseable, se registra como pedido
 *       {@code REJECTED} (razón {@code DESERIALIZATION ...}) conservando los identificadores que
 *       sí se pudieron leer (salvage) y el payload crudo como trazabilidad.</li>
 *   <li>Mapear el DTO externo al comando del dominio, sin exponer Jackson ni Kafka al dominio.</li>
 *   <li>Invocar el caso de uso: él valida el contrato de entrada, evalúa elegibilidad, calcula,
 *       persiste y publica el resultado en {@code orders.processed.v1}.</li>
 * </ol>
 *
 * <p>Todo mensaje que entra termina persistido y publicado con su estado final; no existe DLT.</p>
 */
@Component
public class OrderCreatedListener {

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedListener.class);

    /** Versión asumida cuando el contrato viene sin {@code eventVersion} (campo opcional). */
    static final long DEFAULT_EVENT_VERSION = 1L;

    /** Claves de correlación del MDC expuestas como campos raíz en los logs JSON. */
    static final String MDC_EVENT_ID = "eventId";
    static final String MDC_ORDER_ID = "orderId";

    /** Prefijo de la razón cuando el payload no pudo deserializarse. */
    private static final String DESERIALIZATION_PREFIX = "DESERIALIZATION: ";

    private final ProcessOrderUseCase processOrderUseCase;
    private final OrderEventJsonReader jsonReader;

    public OrderCreatedListener(ProcessOrderUseCase processOrderUseCase,
                                OrderEventJsonReader jsonReader) {
        this.processOrderUseCase = processOrderUseCase;
        this.jsonReader = jsonReader;
    }

    /**
     * Punto de entrada del evento de pedido.
     *
     * <p>El offset se confirma automáticamente ({@code ack-mode: record}) solo cuando este
     * método termina sin excepción; cualquier fallo escala al manejador de errores de Kafka.</p>
     *
     * @param record Registro crudo: clave recomendada {@code orderId}, valor JSON del evento
     */
    @KafkaListener(id = "ordersCreatedListener", topics = "${app.kafka.topics.orders-created:orders.created.v1}")
    public void onMessage(ConsumerRecord<String, String> record) {
        OrderCreatedEventDto event;
        try {
            event = jsonReader.readEvent(record.value());
        }
        catch (IllegalArgumentException ex) {
            recordUnprocessable(record, DESERIALIZATION_PREFIX + ex.getMessage());
            return;
        }

        // Correlación en el MDC: cada log de este procesamiento lleva eventId/orderId
        // como campos raíz del JSON (ver logback-spring.xml) para consultarlos en Grafana.
        if (event.eventId() != null) {
            MDC.put(MDC_EVENT_ID, event.eventId());
        }
        if (event.orderId() != null) {
            MDC.put(MDC_ORDER_ID, event.orderId());
        }
        try {
            if (event.eventVersion() == null) {
                log.warn("Evento eventId={} orderId={} sin eventVersion; se asume {}", event.eventId(),
                        event.orderId(), DEFAULT_EVENT_VERSION);
            }
            log.debug("Evento recibido eventId={} orderId={} clientId={} items={}", event.eventId(), event.orderId(),
                    event.clientId(), event.items() == null ? 0 : event.items().size());

            processOrderUseCase.processOrder(toCommand(event));
        }
        finally {
            MDC.remove(MDC_EVENT_ID);
            MDC.remove(MDC_ORDER_ID);
        }
    }

    /**
     * Un payload que no pudo parsearse se registra como pedido no procesable: se conserva lo que
     * se pudo leer (orderId del payload, luego la clave del registro, luego un id sintético
     * determinístico por partición/offset para que la re-entrega sea idempotente), el payload
     * crudo y la razón de deserialización.
     */
    private void recordUnprocessable(ConsumerRecord<String, String> record, String reason) {
        String payload = record.value();
        UnprocessableCommand command = unprocessableCommand(jsonReader, record, payload, reason);
        try {
            if (command.orderId() != null) {
                MDC.put(MDC_ORDER_ID, command.orderId());
            }
            if (command.eventId() != null) {
                MDC.put(MDC_EVENT_ID, command.eventId());
            }
            log.warn("Mensaje no parseable orderId={} eventId={}: {}", command.orderId(), command.eventId(),
                    command.reason());
            processOrderUseCase.recordUnprocessable(command);
        }
        finally {
            MDC.remove(MDC_ORDER_ID);
            MDC.remove(MDC_EVENT_ID);
        }
    }

    /**
     * Salvage de identificadores para un mensaje no procesable. Compartido con el recoverer de
     * reintentos agotados: ambos caminos de entrada fallida producen el mismo comando.
     */
    static UnprocessableCommand unprocessableCommand(OrderEventJsonReader reader,
                                                     ConsumerRecord<?, ?> record, String payload,
                                                     String reason) {
        String synthetic = syntheticId(record);
        return new UnprocessableCommand(
                reader.readTextField(payload, "orderId").orElse(keyOf(record, synthetic)),
                reader.readTextField(payload, "eventId").orElse(synthetic),
                reader.readTextField(payload, "market").orElse(null),
                reader.readTextField(payload, "currency").orElse(null),
                reader.readTextField(payload, "clientId").orElse(null),
                reason,
                payload);
    }

    /** Id sintético determinístico por tópico/partición/offset: idempotente ante re-entrega. */
    private static String syntheticId(ConsumerRecord<?, ?> record) {
        return record.topic() + "-" + record.partition() + "-" + record.offset();
    }

    private static String keyOf(ConsumerRecord<?, ?> record, String synthetic) {
        Object key = record.key();
        return key == null || ((String) key).isBlank() ? synthetic : (String) key;
    }

    /** Mapeo explícito DTO del contrato → comando del dominio. */
    static ProcessOrderCommand toCommand(OrderCreatedEventDto event) {
        List<OrderLineCommand> items = event.items() == null
                ? List.of()
                : event.items().stream()
                        .map(item -> new OrderLineCommand(item.productId(), item.quantity(), item.unitPrice()))
                        .toList();

        return new ProcessOrderCommand(
                event.eventId(),
                event.eventVersion() == null ? DEFAULT_EVENT_VERSION : event.eventVersion(),
                event.orderId(),
                event.market(),
                event.currency(),
                event.clientId(),
                event.channel(),
                items);
    }
}