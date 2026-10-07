package jeremias.santacruz.order_processor.infrastructure.adapter.in.kafka;

import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Las 7 reglas de validación de entrada de la sección 5.A.
 */
class OrderCreatedEventValidatorTest {

    private static ValidatorFactory factory;
    private static OrderCreatedEventValidator validator;

    @BeforeAll
    static void setUpValidator() {
        factory = Validation.buildDefaultValidatorFactory();
        validator = new OrderCreatedEventValidator(factory.getValidator());
    }

    @AfterAll
    static void closeFactory() {
        factory.close();
    }

    private static OrderCreatedEventDto.OrderItemDto item(String productId, int quantity, BigDecimal unitPrice) {
        return new OrderCreatedEventDto.OrderItemDto(productId, quantity, unitPrice);
    }

    private static OrderCreatedEventDto event(String market, String currency,
                                              List<OrderCreatedEventDto.OrderItemDto> items) {
        return new OrderCreatedEventDto("01J8ZP6M5E4RH0K7Y2N9A3TQWX", 1L, "ORD-MX-000147", market, currency,
                "CLI-99821", "C1", items);
    }

    private static OrderCreatedEventDto validEvent() {
        return event("MX", "MXN", List.of(item("PRD-001", 24, new BigDecimal("35.5"))));
    }

    @Test
    @DisplayName("Un evento válido no produce violaciones")
    void shouldAcceptValidEvent() {
        assertThat(validator.validate(validEvent())).isEmpty();
    }

    @Test
    @DisplayName("Regla 1: los campos obligatorios en blanco producen violación")
    void shouldRejectMissingRequiredFields() {
        OrderCreatedEventDto event = new OrderCreatedEventDto("evt", 1L, "  ", "MX", "MXN", "", "C1",
                List.of(item("PRD-001", 1, BigDecimal.ONE)));

        List<String> violations = validator.validate(event);

        assertThat(violations)
                .anyMatch(v -> v.contains("orderId"))
                .anyMatch(v -> v.contains("clientId"));
    }

    @Test
    @DisplayName("Regla 2: items debe tener al menos 1 elemento")
    void shouldRejectEmptyItems() {
        List<String> violations = validator.validate(event("MX", "MXN", List.of()));

        assertThat(violations).anyMatch(v -> v.contains("items"));
    }

    @Test
    @DisplayName("Regla 3: no se permiten productId duplicados en el mismo pedido")
    void shouldRejectDuplicateProductIds() {
        OrderCreatedEventDto event = event("MX", "MXN",
                List.of(item("PRD-001", 1, BigDecimal.ONE), item("PRD-001", 2, BigDecimal.ONE)));

        List<String> violations = validator.validate(event);

        assertThat(violations).anyMatch(v -> v.contains("productId duplicado"));
    }

    @Test
    @DisplayName("Regla 4: quantity debe ser mayor que 0")
    void shouldRejectNonPositiveQuantity() {
        List<String> violations = validator.validate(event("MX", "MXN",
                List.of(item("PRD-001", 0, BigDecimal.ONE), item("PRD-002", -3, BigDecimal.ONE))));

        assertThat(violations).anyMatch(v -> v.contains("quantity"));
    }

    @Test
    @DisplayName("Regla 5: unitPrice debe ser mayor o igual que 0")
    void shouldRejectNegativeUnitPrice() {
        List<String> violations = validator.validate(event("MX", "MXN",
                List.of(item("PRD-001", 1, new BigDecimal("-0.01")))));

        assertThat(violations).anyMatch(v -> v.contains("unitPrice"));
    }

    @Test
    @DisplayName("Regla 6: mercados válidos MX, CO, PE")
    void shouldRejectUnknownMarket() {
        List<String> violations = validator.validate(event("AR", "MXN",
                List.of(item("PRD-001", 1, BigDecimal.ONE))));

        assertThat(violations).anyMatch(v -> v.contains("market"));
    }

    @Test
    @DisplayName("Regla 7: la moneda debe corresponder al mercado")
    void shouldRejectCurrencyThatDoesNotMatchMarket() {
        List<String> violations = validator.validate(event("MX", "COP",
                List.of(item("PRD-001", 1, BigDecimal.ONE))));

        assertThat(violations).anyMatch(v -> v.contains("no corresponde al market"));
    }

    @Test
    @DisplayName("Regla 7: las combinaciones válidas MX/MXN, CO/COP y PE/PEN pasan")
    void shouldAcceptValidMarketCurrencyCombinations() {
        assertThat(validator.validate(event("MX", "MXN", List.of(item("P1", 1, BigDecimal.ONE))))).isEmpty();
        assertThat(validator.validate(event("CO", "COP", List.of(item("P1", 1, BigDecimal.ONE))))).isEmpty();
        assertThat(validator.validate(event("PE", "PEN", List.of(item("P1", 1, BigDecimal.ONE))))).isEmpty();
    }

    @Test
    @DisplayName("Evento nulo produce una violación de payload")
    void shouldRejectNullEvent() {
        assertThat(validator.validate(null)).hasSize(1);
    }

    @Test
    @DisplayName("Los productos duplicados se reportan una vez por repetición")
    void shouldReportEachDuplicateOnce() {
        OrderCreatedEventDto event = event("MX", "MXN", List.of(
                item("PRD-001", 1, BigDecimal.ONE),
                item("PRD-001", 1, BigDecimal.ONE),
                item("PRD-001", 1, BigDecimal.ONE)));

        List<String> violations = validator.validate(event);

        assertThat(violations).filteredOn(v -> v.contains("productId duplicado")).hasSize(2);
    }
}
