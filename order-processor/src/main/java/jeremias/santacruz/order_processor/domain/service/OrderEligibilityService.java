package jeremias.santacruz.order_processor.domain.service;

import jeremias.santacruz.order_processor.domain.model.client.Client;
import jeremias.santacruz.order_processor.domain.model.order.Market;
import jeremias.santacruz.order_processor.domain.model.product.Product;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class OrderEligibilityService {

    public Optional<String> evaluateEligibility(Client client, Map<String, Product> products, Market orderMarket) {
        if (!client.isActive()) {
            return Optional.of("CLIENT_INACTIVE: Client " + client.clientId() + " is not ACTIVE");
        }

        if (!Objects.equals(client.market(), orderMarket)) {
            return Optional.of(String.format("MARKET_MISMATCH: Client market (%s) does not match Order market (%s)", client.market(), orderMarket));
        }

        return products.entrySet().stream()
                .filter(entry -> entry.getValue() == null || !entry.getValue().isActive())
                .findFirst()
                .map(entry -> entry.getValue() == null
                        ? "PRODUCT_NOT_FOUND: Product " + entry.getKey() + " does not exist"
                        : "PRODUCT_INACTIVE: Product " + entry.getKey() + " is DISCONTINUED");
    }
}
