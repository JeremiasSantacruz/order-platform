package jeremias.santacruz.order_processor.infrastructure.adapter.in.kafka;

import jeremias.santacruz.order_processor.domain.port.in.ProcessOrderUseCase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.stereotype.Component;

/**
 * Última instancia del manejador de errores: se invoca cuando se agotan los reintentos con
 * backoff de un fallo transitorio (429/5xx/timeout) o ante cualquier fallo interno repetido.
 *
 * <p>El resultado se registra con el caso de uso y se persiste/publica, igual que cualquier otro
 * pedido: un fallo de reintentos agotados es un estado {@code TECHNICAL_FAILURE} en el documento
 * y en {@code orders.processed.v1}. No hay DLT: el mensaje sale del flujo porque su offset se
 * confirma recién después de persistir y publicar.</p>
 *
 * <p>Si el payload ya no es parseable (no debería ocurrir: los mensajes no parseables se atienden
 * en el listener), se registra como pedido no procesable con el mismo salvage de
 * {@link OrderCreatedListener}.</p>
 */
@Component
public class ProcessingFailureRecoverer implements ConsumerRecordRecoverer {

    private static final Logger log = LoggerFactory.getLogger(ProcessingFailureRecoverer.class);

    private final ProcessOrderUseCase processOrderUseCase;
    private final OrderEventJsonReader jsonReader;

    public ProcessingFailureRecoverer(ProcessOrderUseCase processOrderUseCase,
                                      OrderEventJsonReader jsonReader) {
        this.processOrderUseCase = processOrderUseCase;
        this.jsonReader = jsonReader;
    }

    @Override
    public void accept(ConsumerRecord<?, ?> record, Exception exception) {
        String payload = record.value() == null ? null : record.value().toString();
        String detail = describe(exception);

        try {
            OrderCreatedEventDto event = jsonReader.readEvent(payload);
            setCorrelation(event.eventId(), event.orderId());
            log.error("Reintentos agotados para eventId={} orderId={}: {}", event.eventId(), event.orderId(),
                    detail);
            processOrderUseCase.recordTechnicalFailure(OrderCreatedListener.toCommand(event), detail);
        }
        catch (IllegalArgumentException unparseable) {
            // Defensivo: un payload que aquí ya no se puede leer se registra como no procesable.
            var command = OrderCreatedListener.unprocessableCommand(jsonReader, record, payload,
                    "RETRIES_EXHAUSTED: " + detail);
            setCorrelation(command.eventId(), command.orderId());
            log.error("Reintentos agotados para un payload no parseable orderId={} eventId={}: {}",
                    command.orderId(), command.eventId(), detail);
            processOrderUseCase.recordUnprocessable(command);
        }
        finally {
            MDC.remove(OrderCreatedListener.MDC_EVENT_ID);
            MDC.remove(OrderCreatedListener.MDC_ORDER_ID);
        }
    }

    private static void setCorrelation(String eventId, String orderId) {
        if (eventId != null) {
            MDC.put(OrderCreatedListener.MDC_EVENT_ID, eventId);
        }
        if (orderId != null) {
            MDC.put(OrderCreatedListener.MDC_ORDER_ID, orderId);
        }
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