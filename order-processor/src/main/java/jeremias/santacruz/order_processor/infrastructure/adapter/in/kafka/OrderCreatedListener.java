package jeremias.santacruz.order_processor.infrastructure.adapter.in.kafka;

import jeremias.santacruz.order_processor.domain.port.in.ProcessOrderUseCase;
import jeremias.santacruz.order_processor.domain.port.in.ProcessOrderUseCase.OrderLineCommand;
import jeremias.santacruz.order_processor.domain.port.in.ProcessOrderUseCase.ProcessOrderCommand;
import jeremias.santacruz.order_processor.domain.service.OrderContractService;
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
 *   <li>Deserializar el payload crudo (si falla, violación de contrato).</li>
 *   <li>Validar las 7 reglas de entrada de la sección 5.A (si falla, violación de contrato).</li>
 *   <li>Mapear el DTO externo al comando del dominio, sin exponer Jackson ni Kafka al dominio.</li>
 *   <li>Invocar el caso de uso y dejar que él devuelva el resultado.</li>
 * </ol>
 *
 * <p>Las violaciones de contrato se lanzan como {@link ContractViolationException}, que el
 * {@code DefaultErrorHandler} clasifica como no reintentable: el mensaje va directo a la DLT
 * {@code orders.processing.dlt} con los headers de metadatos exigidos por la sección 7.</p>
 */
@Component
public class OrderCreatedListener {

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedListener.class);

    /** Versión asumida cuando el contrato viene sin {@code eventVersion} (campo opcional). */
    static final long DEFAULT_EVENT_VERSION = 1L;

    /** Claves de correlación del MDC expuestas como campos raíz en los logs JSON. */
    static final String MDC_EVENT_ID = "eventId";
    static final String MDC_ORDER_ID = "orderId";

    private final ProcessOrderUseCase processOrderUseCase;
    private final OrderEventJsonReader jsonReader;

    /** Reglas del contrato de entrada (5.A): lógica pura de dominio. */
    private final OrderContractService contract = new OrderContractService();

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
            throw new ContractViolationException(ContractViolationException.Category.DESERIALIZATION,
                    "payload no interpretable: " + ex.getMessage());
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
            ProcessOrderCommand command = toCommand(event);

            // 5.A: las reglas del contrato de entrada son no recuperables → DLT directa.
            List<String> violations = contract.validate(command);
            if (!violations.isEmpty()) {
                throw new ContractViolationException(ContractViolationException.Category.CONTRACT_VIOLATION,
                        String.join("; ", violations));
            }

            if (event.eventVersion() == null) {
                log.warn("Evento eventId={} orderId={} sin eventVersion; se asume {}", event.eventId(),
                        event.orderId(), DEFAULT_EVENT_VERSION);
            }
            log.debug("Evento recibido eventId={} orderId={} clientId={} items={}", event.eventId(), event.orderId(),
                    event.clientId(), event.items() == null ? 0 : event.items().size());

            processOrderUseCase.processOrder(command);
        }
        finally {
            MDC.remove(MDC_EVENT_ID);
            MDC.remove(MDC_ORDER_ID);
        }
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
