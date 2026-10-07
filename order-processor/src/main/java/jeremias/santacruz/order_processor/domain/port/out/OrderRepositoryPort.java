package jeremias.santacruz.order_processor.domain.port.out;

import jeremias.santacruz.order_processor.domain.model.order.Order;

import java.util.Optional;

public interface OrderRepositoryPort {

        /**
         * Busca un pedido por su ID de negocio.
         */
        Optional<Order> findByOrderId(String orderId);

        /**
         * Persiste o actualiza un pedido en la base de datos.
         * Debe respetar el control de concurrencia por eventVersion.
         *
         * @param order Agregado Order a guardar
         * @return El pedido guardado
         */
        Order save(Order order);

        /**
         * Verifica la existencia previa de un evento para evitar duplicados.
         */
        boolean existsByEventId(String eventId);
}
