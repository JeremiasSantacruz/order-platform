package jeremias.santacruz.order_processor.infrastructure.adapter.in.kafka;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Valida el contrato de entrada {@code orders.created.v1} (sección 5.A de Especificaciones.md).
 *
 * <p>Combina las restricciones declaradas en {@link OrderCreatedEventDto} (Bean Validation) con
 * las reglas cruzadas que no pueden expresarse anotación a anotación. Devuelve la lista de
 * motivos de rechazo; una lista no vacía convierte el mensaje en un
 * {@link ContractViolationException} (definitivo, sin reintentos, a la DLT).</p>
 *
 * <p>Reglas cubiertas:</p>
 * <ol>
 *   <li>1 - Campos obligatorios.</li>
 *   <li>2 - {@code items} con al menos 1 elemento.</li>
 *   <li>3 - Sin {@code productId} duplicados en el mismo pedido.</li>
 *   <li>4 - {@code quantity} &gt; 0.</li>
 *   <li>5 - {@code unitPrice} &gt;= 0.</li>
 *   <li>6 - Mercados válidos: MX, CO, PE.</li>
 *   <li>7 - La moneda corresponde al mercado (MXN/MX, COP/CO, PEN/PE).</li>
 * </ol>
 */
@Component
public class OrderCreatedEventValidator {

    /** Matriz mercado/moneda de la regla 7. */
    private static final Map<String, String> CURRENCY_BY_MARKET = Map.of(
            "MX", "MXN",
            "CO", "COP",
            "PE", "PEN"
    );

    private final jakarta.validation.Validator beanValidator;

    public OrderCreatedEventValidator(jakarta.validation.Validator beanValidator) {
        this.beanValidator = beanValidator;
    }

    /**
     * Ejecuta las 7 reglas de validación de entrada.
     *
     * @param event Evento ya deserializado
     * @return motivos de rechazo; vacío cuando el mensaje es válido
     */
    public List<String> validate(OrderCreatedEventDto event) {
        if (event == null) {
            return List.of("payload vacío o no convertible a OrderCreatedEventDto");
        }

        List<String> violations = new ArrayList<>();
        beanValidator.validate(event)
                .forEach(violation -> violations.add(violation.getPropertyPath() + " " + violation.getMessage()));

        // Regla 3: productId duplicados dentro del mismo pedido.
        Set<String> seenProducts = new HashSet<>();
        if (event.items() != null) {
            for (OrderCreatedEventDto.OrderItemDto item : event.items()) {
                if (item == null || item.productId() == null || item.productId().isBlank()) {
                    continue; // ya reportado por Bean Validation (regla 1)
                }
                if (!seenProducts.add(item.productId())) {
                    violations.add("items productId duplicado: " + item.productId());
                }
            }
        }

        // Regla 7: la moneda debe corresponder al mercado. Se evalúa solo cuando el mercado es
        // válido para no duplicar el reporte de la regla 6.
        String expectedCurrency = CURRENCY_BY_MARKET.get(event.market());
        if (expectedCurrency != null && !expectedCurrency.equals(event.currency())) {
            violations.add("currency " + event.currency() + " no corresponde al market " + event.market()
                    + " (esperado " + expectedCurrency + ")");
        }

        return violations;
    }
}