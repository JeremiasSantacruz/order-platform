package jeremias.santacruz.order_processor.infrastructure.adapter.out.mongo;

import jeremias.santacruz.order_processor.domain.model.client.Client;
import jeremias.santacruz.order_processor.domain.model.order.EnrichedOrderLine;
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
 * <p>Los campos nuevos ({@code client}, {@code enrichedLines}, {@code sourcePayload}) son
 * aditivos: los documentos escritos por versiones anteriores simplemente los traen en {@code null}
 * y el mapeo al agregado los tolera, así que no hace falta migración.</p>
 *
 * @param orderId        Identificador de negocio; clave primaria del documento
 * @param eventId        Último evento aplicado (idempotencia, sección 7.1)
 * @param eventVersion   Versión aplicada; base del control de concurrencia (7.2 / 7.3)
 * @param market         Mercado del pedido; {@code null} cuando el payload no lo traía o era inválido
 * @param currency       Moneda del pedido; {@code null} cuando el payload no la traía o era inválida
 * @param clientId       Cliente del pedido; {@code null} cuando el payload no lo traía
 * @param status         APPROVED, REJECTED o TECHNICAL_FAILURE
 * @param totals         Totales finales; cero en rechazos y fallos técnicos
 * @param rejectionReason Razón de rechazo/fallo técnico, si aplica
 * @param client         Snapshot del cliente consultado al procesar; {@code null} si nunca se resolvió
 * @param enrichedLines  Líneas con contexto fiscal/comercial e importes calculados (solo APPROVED)
 * @param sourcePayload  Payload crudo del mensaje; solo en mensajes que no pudieron procesarse
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
        Instant processedAt,
        Client client,
        List<EnrichedOrderLine> enrichedLines,
        String sourcePayload
) {}
