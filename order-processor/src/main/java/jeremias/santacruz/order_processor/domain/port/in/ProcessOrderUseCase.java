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

        /**
         * Registra un mensaje que no pudo interpretarse (payload no parseable) como pedido
         * {@code REJECTED}, lo persiste y publica su resultado.
         *
         * <p>No hay control de versión que aplicar: el mensaje no llegó a tener versión utilizable,
         * por lo que el documento se guarda con {@code eventVersion} 0 y cualquier evento válido
         * posterior lo sustituye.</p>
         *
         * @param command Identificadores ya resueltos por la entrada y motivo del fallo
         * @return El pedido registrado, o el resultado vigente si otro evento ganó la persistencia
         */
        Order recordUnprocessable(UnprocessableCommand command);

        /**
         * Registra el resultado técnico de un mensaje cuyos reintentos transitorios se agotaron
         * (429, 5xx o timeout tras el backoff). El payload sí es válido, así que se construye el
         * pedido completo, se marca {@code TECHNICAL_FAILURE}, se persiste y se publica.
         *
         * @param command Comando de entrada del mensaje
         * @param detail  Detalle del fallo; la razón persistida es {@code TECHNICAL_FAILURE: detail}
         * @return El pedido procesado en su estado final
         */
        Order recordTechnicalFailure(ProcessOrderCommand command, String detail);

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

        /**
         * Mensaje de entrada que no pudo deserializarse.
         *
         * @param orderId       orderId del payload, o la clave del registro Kafka, o uno sintético
         * @param eventId       eventId del payload, o uno sintético determinístico
         * @param market        Mercado si se pudo leer y es válido, {@code null} si no
         * @param currency      Moneda si se pudo leer y es válida, {@code null} si no
         * @param clientId      Cliente si se pudo leer, {@code null} si no
         * @param reason        Motivo del descarte (p. ej. {@code DESERIALIZATION: JSON inválido: ...})
         * @param sourcePayload Payload crudo conservado como trazabilidad
         */
        record UnprocessableCommand(
                String orderId,
                String eventId,
                String market,
                String currency,
                String clientId,
                String reason,
                String sourcePayload
        ) {}
}
