package jeremias.santacruz.order_processor.infrastructure.adapter.out.kafka;

import java.math.BigDecimal;

/**
 * Contrato JSON del evento de salida {@code orders.processed.v1} (sección 5.D de
 * Especificaciones.md).
 *
 * <p>Se serializa con los nombres exactos de la especificación ({@code tax} y {@code grandTotal}
 * en los totales). {@code occurredAt} se serializa como texto ISO-8601 para no requerir módulos
 * de fecha/hora de Jackson.</p>
 *
 * @param eventId        Identificador del evento de salida: {@code <sourceEventId>-OUT}
 * @param eventVersion   Versión del contrato de salida (tópico {@code orders.processed.v1})
 * @param occurredAt     Momento de publicación en ISO-8601
 * @param sourceEventId  Evento de entrada que originó este resultado (permite deduplicar)
 * @param orderId        Identificador de negocio del pedido
 * @param status         Estado final: APPROVED, REJECTED o TECHNICAL_FAILURE
 * @param market         Mercado del pedido
 * @param currency       Moneda del pedido
 * @param totals         Totales calculados (cero cuando el pedido fue rechazado)
 * @param reason         Razón del rechazo/fallo técnico, o {@code null} si fue aprobado
 */
record OrderProcessedEventDto(
        String eventId,
        long eventVersion,
        String occurredAt,
        String sourceEventId,
        String orderId,
        String status,
        String market,
        String currency,
        TotalsDto totals,
        String reason
) {

    /** Totales con los nombres del contrato de salida (sección 5.D). */
    record TotalsDto(
            BigDecimal grossSubtotal,
            BigDecimal discount,
            BigDecimal netSubtotal,
            BigDecimal tax,
            BigDecimal grandTotal
    ) {}
}
