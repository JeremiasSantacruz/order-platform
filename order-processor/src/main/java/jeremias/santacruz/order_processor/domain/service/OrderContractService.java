package jeremias.santacruz.order_processor.domain.service;

import jeremias.santacruz.order_processor.domain.model.order.Currency;
import jeremias.santacruz.order_processor.domain.model.order.Market;
import jeremias.santacruz.order_processor.domain.port.in.ProcessOrderUseCase.OrderLineCommand;
import jeremias.santacruz.order_processor.domain.port.in.ProcessOrderUseCase.ProcessOrderCommand;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

/**
 * Reglas de validación de entrada del contrato {@code orders.created.v1} (sección 5.A).
 *
 * <p>Viven en el dominio y se ejecutan dentro del caso de uso, junto a la elegibilidad: una
 * violación no es un fallo del mensaje sino un resultado de negocio, por lo que se traduce en un
 * pedido {@code REJECTED} con la razón explícita, que se persiste y se publica igual que cualquier
 * otro resultado.</p>
 *
 * <p>Reglas cubiertas:</p>
 * <ol>
 *   <li>1 - Campos obligatorios: {@code eventId}, {@code orderId}, {@code market}, {@code currency},
 *       {@code clientId} y {@code items}.</li>
 *   <li>2 - {@code items} con al menos 1 elemento.</li>
 *   <li>3 - Sin {@code productId} duplicados en el mismo pedido.</li>
 *   <li>4 - {@code quantity} &gt; 0.</li>
 *   <li>5 - {@code unitPrice} &gt;= 0.</li>
 *   <li>6 - Mercados válidos: MX, CO, PE.</li>
 *   <li>7 - La moneda corresponde al mercado (MXN/MX, COP/CO, PEN/PE).</li>
 * </ol>
 */
public class OrderContractService {

    /** Matriz mercado/moneda de la regla 7. */
    private static final Map<String, String> CURRENCY_BY_MARKET = Map.of(
            "MX", "MXN",
            "CO", "COP",
            "PE", "PEN"
    );

    /**
     * Ejecuta las 7 reglas de validación de entrada.
     *
     * @param command Comando de entrada ya deserializado (puede traer campos ausentes o inválidos)
     * @return motivos de rechazo; lista vacía cuando el mensaje es válido
     */
    public List<String> validate(ProcessOrderCommand command) {
        if (command == null) {
            return List.of("payload vacío o no convertible a OrderCreatedEventDto");
        }

        List<String> violations = new ArrayList<>();

        // Regla 1: campos obligatorios.
        if (isBlank(command.eventId())) {
            violations.add("eventId es obligatorio");
        }
        if (isBlank(command.orderId())) {
            violations.add("orderId es obligatorio");
        }
        boolean marketKnown = !isBlank(command.market());
        if (!marketKnown) {
            violations.add("market es obligatorio");
        }
        else if (parseMarket(command.market()) == null) {
            violations.add("market debe ser MX, CO o PE");
        }
        if (isBlank(command.currency())) {
            violations.add("currency es obligatorio");
        }
        if (isBlank(command.clientId())) {
            violations.add("clientId es obligatorio");
        }

        // Reglas 2 a 5: líneas del pedido.
        if (command.items() == null || command.items().isEmpty()) {
            violations.add("items debe tener al menos 1 elemento");
        }
        else {
            violations.addAll(validateItems(command.items()));
        }

        // Regla 7: la moneda debe corresponder al mercado. Se evalúa solo cuando el mercado es
        // válido para no duplicar el reporte de la regla 6.
        Market market = parseMarket(command.market());
        if (market != null && !isBlank(command.currency())) {
            String expected = CURRENCY_BY_MARKET.get(command.market());
            if (expected != null && !expected.equals(command.currency())) {
                violations.add("currency " + command.currency() + " no corresponde al market "
                        + command.market() + " (esperado " + expected + ")");
            }
        }

        return List.copyOf(violations);
    }

    /**
     * Reglas 3 a 5 para cada línea. El flujo es secuencial y {@code seenProducts} conserva el
     * orden de aparición: cada repetición de un {@code productId} reporta una violación en la
     * posición del ítem repetido, preservando el orden de los mensajes de rechazo.
     */
    private List<String> validateItems(List<OrderLineCommand> items) {
        Set<String> seenProducts = new HashSet<>();
        return IntStream.range(0, items.size())
                .boxed()
                .flatMap(i -> validateItem(i, items.get(i), seenProducts).stream())
                .toList();
    }

    /** Valida una línea y devuelve sus violaciones en el orden de las reglas 3, 4 y 5. */
    private List<String> validateItem(int index, OrderLineCommand item, Set<String> seenProducts) {
        if (item == null) {
            return List.of("items[" + index + "] es obligatorio");
        }
        List<String> violations = new ArrayList<>(3);
        if (isBlank(item.productId())) {
            violations.add("items[" + index + "].productId es obligatorio");
        }
        else if (!seenProducts.add(item.productId())) {
            violations.add("items productId duplicado: " + item.productId());
        }
        if (item.quantity() <= 0) {
            violations.add("items[" + index + "].quantity debe ser un entero mayor que 0");
        }
        if (item.unitPrice() == null) {
            violations.add("items[" + index + "].unitPrice es obligatorio");
        }
        else if (item.unitPrice().signum() < 0) {
            violations.add("items[" + index + "].unitPrice debe ser mayor o igual que 0");
        }
        return violations;
    }

    /** Mercado si es un valor del enum, o {@code null} si no lo es (regla 6). */
    public static Market parseMarket(String market) {
        if (isBlank(market)) {
            return null;
        }
        try {
            return Market.valueOf(market);
        }
        catch (IllegalArgumentException ex) {
            return null;
        }
    }

    /** Moneda si es un valor del enum, o {@code null} si no lo es. */
    public static Currency parseCurrency(String currency) {
        if (isBlank(currency)) {
            return null;
        }
        try {
            return Currency.valueOf(currency);
        }
        catch (IllegalArgumentException ex) {
            return null;
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
