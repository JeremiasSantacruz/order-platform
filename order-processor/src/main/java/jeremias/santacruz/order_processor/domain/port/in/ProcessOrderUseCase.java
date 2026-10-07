package jeremias.santacruz.order_processor.domain.port.in;

import jeremias.santacruz.order_processor.domain.model.order.Order;

import java.math.BigDecimal;
import java.util.List;

public interface ProcessOrderUseCase {


        /**
         * Procesa un evento de pedido entrante aplicando validaciones,
         * consultas de contexto externo, cálculo de totales y persistencia.
         *
         * @param command Comando que encapsula los datos del evento recibido
         * @return El pedido procesado en su estado final (APPROVED, REJECTED o TECHNICAL_FAILURE)
         */
        Order processOrder(ProcessOrderCommand command);

        // DTO / Record de entrada al Caso de Uso (independiente de Kafka o JSON)
        record ProcessOrderCommand(
                String eventId,
                long eventVersion,
                String orderId,
                String market,
                String currency,
                String clientId,
                String channel,
                List<OrderLineCommand> items
        ) {}

        record OrderLineCommand(
                String productId,
                int quantity,
                BigDecimal unitPrice
        ) {}
}
