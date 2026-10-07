package jeremias.santacruz.order_processor.domain.model.client;

import jeremias.santacruz.order_processor.domain.model.order.Market;

public record Client(
        String clientId,
        String name,
        ClientStatus status,
        ClientSegment segment,
        TaxRegime taxRegime,
        Market market
) {
    public boolean isActive() {
        return status == ClientStatus.ACTIVE;
    }

    public boolean isExempt() {
        return taxRegime == TaxRegime.EXEMPT;
    }

    public boolean isWholesale() {
        return segment == ClientSegment.WHOLESALE;
    }
}

