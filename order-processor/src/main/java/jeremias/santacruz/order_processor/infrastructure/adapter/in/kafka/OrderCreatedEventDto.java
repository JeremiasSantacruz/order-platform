package jeremias.santacruz.order_processor.infrastructure.adapter.in.kafka;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import jakarta.validation.constraints.Pattern;

import java.math.BigDecimal;
import java.util.List;

/**
 * Contrato JSON del evento de entrada {@code orders.created.v1} (Sección 5.A de Especificaciones.md).
 *
 * <p>Este registro es propiedad de la infraestructura: traduce literalmente el contrato externo,
 * incluidas las reglas de validación que se expresan como restricciones de Bean Validation.
 * Las reglas cruzadas (productos duplicados, mercado/moneda) viven en
 * {@link OrderCreatedEventValidator}.</p>
 *
 * <p>Reglas cubiertas aquí:</p>
 * <ol>
 *   <li>1 - Campos obligatorios: {@code eventId}, {@code orderId}, {@code market}, {@code currency},
 *       {@code clientId} y {@code items}.</li>
 *   <li>2 - {@code items} debe tener al menos un elemento.</li>
 *   <li>4 - {@code quantity} &gt; 0 (entero; los decimales se rechazan al deserializar).</li>
 *   <li>5 - {@code unitPrice} &gt;= 0.</li>
 *   <li>6 - Mercados válidos: MX, CO, PE.</li>
 * </ol>
 *
 * @param eventId    Identificador único e idempotente del evento
 * @param eventVersion Versión del evento; no es obligatoria en el contrato, si falta se asume 1
 * @param orderId    Identificador de negocio del pedido (clave recomendada del tópico)
 * @param market     Mercado del pedido: MX, CO o PE
 * @param currency   Moneda del pedido, debe corresponder al mercado (regla 7)
 * @param clientId   Identificador de cliente
 * @param channel    Canal comercial (opcional)
 * @param items      Líneas del pedido, al menos una
 */
public record OrderCreatedEventDto(
        @NotBlank(message = "eventId es obligatorio")
        String eventId,

        Long eventVersion,

        @NotBlank(message = "orderId es obligatorio")
        String orderId,

        @NotBlank(message = "market es obligatorio")
        @Pattern(regexp = "MX|CO|PE", message = "market debe ser MX, CO o PE")
        String market,

        @NotBlank(message = "currency es obligatorio")
        String currency,

        @NotBlank(message = "clientId es obligatorio")
        String clientId,

        String channel,

        @NotEmpty(message = "items debe tener al menos 1 elemento")
        @Valid
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
            @NotBlank(message = "items[].productId es obligatorio")
            String productId,

            @Positive(message = "items[].quantity debe ser un entero mayor que 0")
            int quantity,

            @NotNull(message = "items[].unitPrice es obligatorio")
            @PositiveOrZero(message = "items[].unitPrice debe ser mayor o igual que 0")
            BigDecimal unitPrice
    ) {}
}