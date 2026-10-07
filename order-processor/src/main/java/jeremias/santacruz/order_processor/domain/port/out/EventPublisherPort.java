package jeremias.santacruz.order_processor.domain.port.out;

import jeremias.santacruz.order_processor.domain.model.order.Order;

import java.time.Instant;

public interface EventPublisherPort {

    /**
     * Publica el evento de resultado del procesamiento en el tópico 'orders.processed.v1'.
     *
     * @param order Pedido con estado final (APPROVED o REJECTED)
     */
    void publishOrderProcessed(Order order);

    /**
     * Publica un mensaje no procesable o con error fatal en el tópico DLT ('orders.processing.dlt').
     *
     * <p>Los campos de {@link DeadLetterInfo} se exponen como headers de metadatos, según la
     * sección 7 de Especificaciones.md: {@code orderId}, {@code eventId}, {@code errorCategory},
     * {@code summaryCause}, {@code attemptCount}, {@code timestamp} y {@code component}.</p>
     *
     * @param rawPayload Contenido original del mensaje consumido (se reenvía sin modificar)
     * @param info       Metadatos del fallo
     */
    void publishToDLT(String rawPayload, DeadLetterInfo info);

    /**
     * Metadatos de un mensaje fallido, para publicarlos como headers de la DLT.
     *
     * @param orderId       Identificador de negocio del pedido, si pudo extraerse del payload
     * @param eventId       Identificador del evento, si pudo extraerse del payload
     * @param errorCategory Categoría técnica del error (p. ej. {@code DESERIALIZATION},
     *                      {@code CONTRACT_VIOLATION}, {@code RETRIES_EXHAUSTED})
     * @param summaryCause  Causa resumida del fallo
     * @param attemptCount  Número de intentos de entrega realizados
     * @param timestamp     Momento en que se descartó el mensaje
     * @param component     Componente que origina el descarte (p. ej. {@code order-processor})
     */
    record DeadLetterInfo(
            String orderId,
            String eventId,
            String errorCategory,
            String summaryCause,
            int attemptCount,
            Instant timestamp,
            String component
    ) {}
}