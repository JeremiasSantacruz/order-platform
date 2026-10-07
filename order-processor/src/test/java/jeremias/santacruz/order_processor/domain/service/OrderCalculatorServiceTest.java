package jeremias.santacruz.order_processor.domain.service;


import jeremias.santacruz.order_processor.domain.model.client.Client;
import jeremias.santacruz.order_processor.domain.model.client.ClientSegment;
import jeremias.santacruz.order_processor.domain.model.client.ClientStatus;
import jeremias.santacruz.order_processor.domain.model.client.TaxRegime;
import jeremias.santacruz.order_processor.domain.model.order.Market;
import jeremias.santacruz.order_processor.domain.model.order.OrderLine;
import jeremias.santacruz.order_processor.domain.model.order.OrderTotals;
import jeremias.santacruz.order_processor.domain.model.product.Product;
import jeremias.santacruz.order_processor.domain.model.product.ProductStatus;
import jeremias.santacruz.order_processor.domain.model.product.TaxCategory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class OrderCalculatorServiceTest {

    private OrderCalculatorService calculator;

    @BeforeEach
    void setUp() {
        // Instanciación directa sin estrategias ni resolvientes
        this.calculator = new OrderCalculatorService();
    }

    @Nested
    @DisplayName("Pruebas de Descuento Mayorista (WHOLESALE)")
    class WholesaleDiscountTests {

        @Test
        @DisplayName("Debe aplicar 3% de descuento cuando cliente es WHOLESALE y la cantidad es >= 20")
        void shouldApplyWholesaleDiscountWhenQuantityIs20OrMore() {
            Client client = new Client("CLI-1", "Distribuidor", ClientStatus.ACTIVE, ClientSegment.WHOLESALE, TaxRegime.GENERAL, Market.MX);
            Product product = new Product("PRD-1", "Refresco", "SKU-1", ProductStatus.ACTIVE, TaxCategory.STANDARD);
            OrderLine item = new OrderLine("PRD-1", 20, new BigDecimal("100.00")); // Gross: 2000.00

            var line = calculator.calculateLine(item, client, product, Market.MX);

            // Gross = 2000.00, Discount 3% = 60.00 -> Net = 1940.00
            assertThat(line.grossSubtotal()).isEqualByComparingTo("2000.00");
            assertThat(line.discount()).isEqualByComparingTo("60.00");
            assertThat(line.netSubtotal()).isEqualByComparingTo("1940.00");
            // Tax MX Standard 16% de 1940.00 = 310.40
            assertThat(line.taxAmount()).isEqualByComparingTo("310.40");
            assertThat(line.lineTotal()).isEqualByComparingTo("2250.40");
        }

        @Test
        @DisplayName("NO debe aplicar descuento si la cantidad es menor a 20 aunque el cliente sea WHOLESALE")
        void shouldNotApplyDiscountWhenQuantityIsLessThan20() {
            Client client = new Client("CLI-1", "Distribuidor", ClientStatus.ACTIVE, ClientSegment.WHOLESALE, TaxRegime.GENERAL, Market.MX);
            Product product = new Product("PRD-1", "Refresco", "SKU-1", ProductStatus.ACTIVE, TaxCategory.STANDARD);
            OrderLine item = new OrderLine("PRD-1", 19, new BigDecimal("100.00")); // Gross: 1900.00

            var line = calculator.calculateLine(item, client, product, Market.MX);

            assertThat(line.discount()).isEqualByComparingTo("0.00");
            assertThat(line.netSubtotal()).isEqualByComparingTo("1900.00");
        }
    }

    @Nested
    @DisplayName("Pruebas de Matriz Impositiva por Mercado y Categoria")
    class TaxMatrixTests {

        @ParameterizedTest(name = "Mercado {0}, Categoría {1} -> Tasa esperada {2}")
        @CsvSource({
                "MX, STANDARD, 0.16",
                "MX, REDUCED,  0.08",
                "MX, EXEMPT,   0.00",
                "CO, STANDARD, 0.19",
                "CO, REDUCED,  0.05",
                "CO, EXEMPT,   0.00",
                "PE, STANDARD, 0.18",
                "PE, REDUCED,  0.10",
                "PE, EXEMPT,   0.00"
        })
        @DisplayName("Debe calcular la tasa de impuesto correcta según el mercado y la categoría fiscal")
        void shouldCalculateCorrectTaxRateForMarketAndCategory(Market market, TaxCategory category, BigDecimal expectedRate) {
            Client client = new Client("CLI-1", "Comercio", ClientStatus.ACTIVE, ClientSegment.RETAIL, TaxRegime.GENERAL, market);
            Product product = new Product("PRD-1", "Producto Test", "SKU-1", ProductStatus.ACTIVE, category);
            OrderLine item = new OrderLine("PRD-1", 10, new BigDecimal("100.00")); // Subtotal = 1000.00

            var line = calculator.calculateLine(item, client, product, market);

            BigDecimal expectedTax = new BigDecimal("1000.00").multiply(expectedRate).setScale(2);
            assertThat(line.taxAmount()).isEqualByComparingTo(expectedTax);
        }

        @Test
        @DisplayName("Debe aplicar tasa 0% cuando el cliente tiene régimen EXEMPT independientemente del producto")
        void shouldApplyZeroTaxWhenClientIsExempt() {
            Client client = new Client("CLI-1", "Cliente Exento", ClientStatus.ACTIVE, ClientSegment.RETAIL, TaxRegime.EXEMPT, Market.CO);
            Product product = new Product("PRD-1", "Producto Estándar", "SKU-1", ProductStatus.ACTIVE, TaxCategory.STANDARD);
            OrderLine item = new OrderLine("PRD-1", 10, new BigDecimal("100.00"));

            var line = calculator.calculateLine(item, client, product, Market.CO);

            assertThat(line.taxAmount()).isEqualByComparingTo("0.00");
        }
    }

    @Nested
    @DisplayName("Pruebas de Redondeo HALF_UP y Suma de Totales")
    class TotalsAndRoundingTests {

        @Test
        @DisplayName("Debe redondear importes a 2 decimales usando HALF_UP y sumar totales correctamente")
        void shouldRoundLineItemsAndAggregateTotalsCorrectly() {
            // Línea 1
            var line1 = new OrderCalculatorService.CalculatedLine(
                    "P1", new BigDecimal("99.99"), BigDecimal.ZERO, new BigDecimal("99.99"), new BigDecimal("16.00"), new BigDecimal("115.99")
            );

            // Línea 2
            var line2 = new OrderCalculatorService.CalculatedLine(
                    "P2", new BigDecimal("21.10"), BigDecimal.ZERO, new BigDecimal("21.10"), new BigDecimal("3.38"), new BigDecimal("24.48")
            );

            OrderTotals totals = calculator.calculateTotals(List.of(line1, line2));

            assertThat(totals.grossSubtotal()).isEqualByComparingTo("121.09");
            assertThat(totals.discount()).isEqualByComparingTo("0.00");
            assertThat(totals.netSubtotal()).isEqualByComparingTo("121.09");
            assertThat(totals.tax()).isEqualByComparingTo("19.38");
            assertThat(totals.grandTotal()).isEqualByComparingTo("140.47");
        }
    }
}