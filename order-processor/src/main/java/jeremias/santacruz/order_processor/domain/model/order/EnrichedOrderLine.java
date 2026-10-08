package jeremias.santacruz.order_processor.domain.model.order;

import jeremias.santacruz.order_processor.domain.model.product.TaxCategory;

import java.math.BigDecimal;

/**
 * Línea del pedido enriquecida con el resultado de la evaluación fiscal y comercial
 * (secciones 6.2 a 6.4).
 *
 * <p>Es lo que se persiste en el documento del pedido como "líneas enriquecidas": conserva los
 * datos de entrada ({@code quantity}, {@code unitPrice}) junto con el contexto que se aplicó
 * ({@code taxCategory}, {@code taxRate}, {@code discountRate}) y los importes calculados, de modo
 * que el documento sea auditable sin tener que recomputar nada.</p>
 *
 * @param productId     Identificador del producto
 * @param quantity      Cantidad solicitada (regla de entrada 4)
 * @param unitPrice     Precio unitario de entrada (regla de entrada 5)
 * @param productName   Nombre del producto al momento del procesamiento
 * @param sku           SKU del producto al momento del procesamiento
 * @param taxCategory   Categoría fiscal del producto
 * @param taxRate       Tasa impositiva efectiva aplicada (0 cuando el cliente es EXEMPT)
 * @param discountRate  Tasa de descuento aplicada (0.03 en mayorista con &ge; 20 unidades)
 * @param grossSubtotal Importe bruto de la línea, redondeado a 2 decimales (HALF_UP)
 * @param discount      Descuento aplicado
 * @param netSubtotal   Importe neto tras el descuento
 * @param taxAmount     Impuesto calculado sobre el neto
 * @param lineTotal     Total de la línea (neto + impuesto)
 */
public record EnrichedOrderLine(
        String productId,
        Integer quantity,
        BigDecimal unitPrice,
        String productName,
        String sku,
        TaxCategory taxCategory,
        BigDecimal taxRate,
        BigDecimal discountRate,
        BigDecimal grossSubtotal,
        BigDecimal discount,
        BigDecimal netSubtotal,
        BigDecimal taxAmount,
        BigDecimal lineTotal
) {}
