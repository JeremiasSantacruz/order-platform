package jeremias.santacruz.order_processor.domain.service;

import jeremias.santacruz.order_processor.domain.port.in.ProcessOrderUseCase.OrderLineCommand;
import jeremias.santacruz.order_processor.domain.port.in.ProcessOrderUseCase.ProcessOrderCommand;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Las 7 reglas de validación de entrada del contrato {@code orders.created.v1}.
 *
 * <p>La validación vive en el dominio y se ejecuta en el caso de uso; una violación se convierte
 * en un pedido {@code REJECTED} con la razón, se persiste y se publica. Por eso este test cubre
 * exclusivamente la lógica pura de {@link OrderContractService}.</p>
 */
class OrderContractServiceTest {

    private final OrderContractService contract = new OrderContractService();

    private static OrderLineCommand item(String productId, int quantity, BigDecimal unitPrice) {
        return new OrderLineCommand(productId, quantity, unitPrice);
    }

    private static ProcessOrderCommand command(String market, String currency,
                                               List<OrderLineCommand> items) {
        return new ProcessOrderCommand("01J8ZP6M5E4RH0K7Y2N9A3TQWX", 1L, "ORD-MX-000147", market, currency,
                "CLI-99821", "C1", items);
    }

    private static ProcessOrderCommand validCommand() {
        return command("MX", "MXN", List.of(item("PRD-001", 24, new BigDecimal("35.5"))));
    }

    @Test
    @DisplayName("Un comando válido no produce violaciones")
    void shouldAcceptValidCommand() {
        assertThat(contract.validate(validCommand())).isEmpty();
    }

    @Test
    @DisplayName("Regla 1: los campos obligatorios en blanco producen violación")
    void shouldRejectMissingRequiredFields() {
        ProcessOrderCommand cmd = new ProcessOrderCommand("evt", 1L, "  ", "MX", "MXN", "", "C1",
                List.of(item("PRD-001", 1, BigDecimal.ONE)));

        List<String> violations = contract.validate(cmd);

        assertThat(violations)
                .anyMatch(v -> v.contains("orderId"))
                .anyMatch(v -> v.contains("clientId"));
    }

    @Test
    @DisplayName("Regla 2: items debe tener al menos 1 elemento")
    void shouldRejectEmptyItems() {
        assertThat(contract.validate(command("MX", "MXN", List.of())))
                .anyMatch(v -> v.contains("items"));
    }

    @Test
    @DisplayName("Regla 3: no se permiten productId duplicados en el mismo pedido")
    void shouldRejectDuplicateProductIds() {
        List<String> violations = contract.validate(command("MX", "MXN",
                List.of(item("PRD-001", 1, BigDecimal.ONE), item("PRD-001", 2, BigDecimal.ONE))));

        assertThat(violations).anyMatch(v -> v.contains("productId duplicado"));
    }

    @Test
    @DisplayName("Regla 4: quantity debe ser mayor que 0")
    void shouldRejectNonPositiveQuantity() {
        List<String> violations = contract.validate(command("MX", "MXN",
                List.of(item("PRD-001", 0, BigDecimal.ONE), item("PRD-002", -3, BigDecimal.ONE))));

        assertThat(violations).anyMatch(v -> v.contains("quantity"));
    }

    @Test
    @DisplayName("Regla 5: unitPrice debe ser mayor o igual que 0")
    void shouldRejectNegativeUnitPrice() {
        List<String> violations = contract.validate(command("MX", "MXN",
                List.of(item("PRD-001", 1, new BigDecimal("-0.01")))));

        assertThat(violations).anyMatch(v -> v.contains("unitPrice"));
    }

    @Test
    @DisplayName("Regla 6: mercados válidos MX, CO, PE")
    void shouldRejectUnknownMarket() {
        assertThat(contract.validate(command("AR", "MXN", List.of(item("PRD-001", 1, BigDecimal.ONE)))))
                .anyMatch(v -> v.contains("market"));
    }

    @Test
    @DisplayName("Regla 7: la moneda debe corresponder al mercado")
    void shouldRejectCurrencyThatDoesNotMatchMarket() {
        assertThat(contract.validate(command("MX", "COP", List.of(item("PRD-001", 1, BigDecimal.ONE)))))
                .anyMatch(v -> v.contains("no corresponde al market"));
    }

    @Test
    @DisplayName("Regla 7: las combinaciones válidas MX/MXN, CO/COP y PE/PEN pasan")
    void shouldAcceptValidMarketCurrencyCombinations() {
        assertThat(contract.validate(command("MX", "MXN", List.of(item("P1", 1, BigDecimal.ONE))))).isEmpty();
        assertThat(contract.validate(command("CO", "COP", List.of(item("P1", 1, BigDecimal.ONE))))).isEmpty();
        assertThat(contract.validate(command("PE", "PEN", List.of(item("P1", 1, BigDecimal.ONE))))).isEmpty();
    }

    @Test
    @DisplayName("Comando nulo produce una violación de payload")
    void shouldRejectNullCommand() {
        assertThat(contract.validate(null)).hasSize(1);
    }

    @Test
    @DisplayName("Los productos duplicados se reportan una vez por repetición")
    void shouldReportEachDuplicateOnce() {
        List<String> violations = contract.validate(command("MX", "MXN", List.of(
                item("PRD-001", 1, BigDecimal.ONE),
                item("PRD-001", 1, BigDecimal.ONE),
                item("PRD-001", 1, BigDecimal.ONE))));

        assertThat(violations).filteredOn(v -> v.contains("productId duplicado")).hasSize(2);
    }
}