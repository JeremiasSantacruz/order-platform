package jeremias.santacruz.order_processor.infrastructure.adapter.in.kafka;

import java.math.BigDecimal;
import java.util.List;

/**
 * Contrato JSON del evento de entrada {@code orders.created.v1} (Sección 5.A de Especificaciones.md).
 *
 * <p>Este registro es propiedad de la infraestructura: traduce literalmente el contrato externo.
 * La validación de las 7 reglas de entrada vive en el dominio
 * ({@link jeremias.santacruz.order_processor.domain.service.OrderContractService}) y se ejecuta
 * dentro del caso de uso: una violación se traduce en un pedido {@code REJECTED} persistido y
 * publicado, no en un fallo del mensaje.</p>
 */
public record OrderCreatedEventDto(
        String eventId,

        Long eventVersion,

        String orderId,

        String market,

        String currency,

        String clientId,

        String channel,

        List<OrderItemDto> items
) {

    /**
     * Línea del pedido dentro del evento de entrada.
     *
     * @param productId  Identificador del producto, único dentro del pedido (regla 3)
     * @param quantity   Cantidad solicitada, entero mayor que cero (regla 4)
     * @param unitPrice  Precio unitario, mayor o igual que cero (regla 5)
     */
    public record OrderItemDto(
            String productId,
            int quantity,
            BigDecimal unitPrice
    ) {}
}