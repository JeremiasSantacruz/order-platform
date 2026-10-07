package jeremias.santacruz.order_processor.domain.service;

import jeremias.santacruz.order_processor.domain.model.client.Client;
import jeremias.santacruz.order_processor.domain.model.client.ClientSegment;
import jeremias.santacruz.order_processor.domain.model.client.ClientStatus;
import jeremias.santacruz.order_processor.domain.model.client.TaxRegime;
import jeremias.santacruz.order_processor.domain.model.order.Market;
import jeremias.santacruz.order_processor.domain.model.product.Product;
import jeremias.santacruz.order_processor.domain.model.product.ProductStatus;
import jeremias.santacruz.order_processor.domain.model.product.TaxCategory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class OrderEligibilityServiceTest {

    private OrderEligibilityService eligibilityService;

    @BeforeEach
    void setUp() {
        this.eligibilityService = new OrderEligibilityService();
    }

    @Test
    @DisplayName("Debe ser elegible (sin razon de rechazo) cuando todo es valido")
    void shouldBeEligibleWhenAllConditionsAreMet() {
        Client client = new Client("CLI-1", "Active Client", ClientStatus.ACTIVE, ClientSegment.RETAIL, TaxRegime.GENERAL, Market.MX);
        Product product = new Product("PRD-1", "Soda", "SKU-1", ProductStatus.ACTIVE, TaxCategory.STANDARD);

        Optional<String> rejection = eligibilityService.evaluateEligibility(client, Map.of("PRD-1", product), Market.MX);

        assertThat(rejection).isEmpty();
    }

    @Test
    @DisplayName("Debe rechazar si el cliente no esta ACTIVE")
    void shouldRejectWhenClientIsInactive() {
        Client client = new Client("CLI-1", "Blocked Client", ClientStatus.BLOCKED, ClientSegment.RETAIL, TaxRegime.GENERAL, Market.MX);
        Product product = new Product("PRD-1", "Soda", "SKU-1", ProductStatus.ACTIVE, TaxCategory.STANDARD);

        Optional<String> rejection = eligibilityService.evaluateEligibility(client, Map.of("PRD-1", product), Market.MX);

        assertThat(rejection).isPresent()
                .get().asString().contains("CLIENT_INACTIVE");
    }

    @Test
    @DisplayName("Debe rechazar si el mercado del cliente difiere del mercado de la orden")
    void shouldRejectWhenMarketMismatch() {
        Client client = new Client("CLI-1", "Client CO", ClientStatus.ACTIVE, ClientSegment.RETAIL, TaxRegime.GENERAL, Market.CO);
        Product product = new Product("PRD-1", "Soda", "SKU-1", ProductStatus.ACTIVE, TaxCategory.STANDARD);

        Optional<String> rejection = eligibilityService.evaluateEligibility(client, Map.of("PRD-1", product), Market.MX);

        assertThat(rejection).isPresent()
                .get().asString().contains("MARKET_MISMATCH");
    }

    @Test
    @DisplayName("Debe rechazar si alguno de los productos no existe (null en mapa)")
    void shouldRejectWhenProductNotFound() {
        Client client = new Client("CLI-1", "Active Client", ClientStatus.ACTIVE, ClientSegment.RETAIL, TaxRegime.GENERAL, Market.MX);
        Map<String, Product> products = new HashMap<>();
        products.put("PRD-99", null);

        Optional<String> rejection = eligibilityService.evaluateEligibility(client, products, Market.MX);

        assertThat(rejection).isPresent()
                .get().asString().contains("PRODUCT_NOT_FOUND");
    }

    @Test
    @DisplayName("Debe rechazar si un producto esta DISCONTINUED")
    void shouldRejectWhenProductIsDiscontinued() {
        Client client = new Client("CLI-1", "Active Client", ClientStatus.ACTIVE, ClientSegment.RETAIL, TaxRegime.GENERAL, Market.MX);
        Product product = new Product("PRD-1", "Old Soda", "SKU-1", ProductStatus.DISCONTINUED, TaxCategory.STANDARD);

        Optional<String> rejection = eligibilityService.evaluateEligibility(client, Map.of("PRD-1", product), Market.MX);

        assertThat(rejection).isPresent()
                .get().asString().contains("PRODUCT_INACTIVE");
    }
}
