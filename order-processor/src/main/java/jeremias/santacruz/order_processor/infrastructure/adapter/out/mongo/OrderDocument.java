package jeremias.santacruz.order_processor.infrastructure.adapter.out.mongo;

import jeremias.santacruz.order_processor.domain.model.order.OrderLine;
import jeremias.santacruz.order_processor.domain.model.order.OrderTotals;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.List;

/**
 * Documento Mongo del agregado {@code Order} (colección {@code orders}).
 *
 * <p>Los importes usan {@code BigDecimal}, que MongoDB almacena como {@code Decimal128}: se
 * conserva la precisión monetaria exigida por la sección 6.4 sin pasar por {@code double}.</p>
 *
 * @param orderId        Identificador de negocio; clave primaria del documento
 * @param eventId        Último evento aplicado (idempotencia, sección 7.1)
 * @param eventVersion   Versión aplicada; base del control de concurrencia (7.2 / 7.3)
 * @param status         APPROVED, REJECTED o TECHNICAL_FAILURE
 * @param totals         Totales finales; {@code null} en fallos técnicos
 * @param rejectionReason Razón de rechazo/fallo técnico, si aplica
 */
@Document(collection = "orders")
public record OrderDocument(
        @Id String orderId,
        String eventId,
        long eventVersion,
        String market,
        String currency,
        String clientId,
        List<OrderLine> items,
        String status,
        OrderTotals totals,
        String rejectionReason,
        Instant receivedAt,
        Instant processedAt
) {}
