package jeremias.santacruz.order_processor.domain.port.out;

import jeremias.santacruz.order_processor.domain.model.order.Order;

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
     * @param rawPayload Contenido original del mensaje consumido
     * @param errorMessage Descripción del error o motivo de la falla
     * @param errorCategory Categoría técnica/sintáctica del error
     * @param retryCount Número de reintentos realizados antes de descartar
     */
    void publishToDLT(String rawPayload, String errorMessage, String errorCategory, int retryCount);
}
