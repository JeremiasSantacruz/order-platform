package jeremias.santacruz.order_processor.domain.port.out;

import jeremias.santacruz.order_processor.domain.model.order.Order;

public interface EventPublisherPort {

    /**
     * Publica el evento de resultado del procesamiento en el tópico 'orders.processed.v1'.
     *
     * <p>Es el único destino de salida de la aplicación: todo mensaje que llega al caso de uso
     * termina persistido en {@code orders} y publicado aquí con su estado final (APPROVED,
     * REJECTED o TECHNICAL_FAILURE). No existe un tópico paralelo para mensajes fallidos.</p>
     *
     * @param order Pedido con estado final
     */
    void publishOrderProcessed(Order order);
}