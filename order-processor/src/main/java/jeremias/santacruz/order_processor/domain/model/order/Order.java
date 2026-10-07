package jeremias.santacruz.order_processor.domain.model.order;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

public class Order {

    private final String orderId;
    private final String eventId;
    private final long eventVersion;
    private final Market market;
    private final Currency currency;
    private final String clientId;
    private final List<OrderLine> items;
    private final Instant receivedAt;

    private OrderStatus status;
    private OrderTotals totals;
    private String rejectionReason;
    private Instant processedAt;

    public Order(String orderId, String eventId, long eventVersion, Market market, Currency currency,
                 String clientId, List<OrderLine> items, Instant receivedAt) {
        this.orderId = Objects.requireNonNull(orderId, "orderId cannot be null");
        this.eventId = Objects.requireNonNull(eventId, "eventId cannot be null");
        this.eventVersion = eventVersion;
        this.market = Objects.requireNonNull(market, "market cannot be null");
        this.currency = Objects.requireNonNull(currency, "currency cannot be null");
        this.clientId = Objects.requireNonNull(clientId, "clientId cannot be null");

        if (items == null || items.isEmpty()) {
            throw new IllegalArgumentException("Order must contain at least one item");
        }
        if (!market.supportsCurrency(currency)) {
            throw new IllegalArgumentException(String.format("Currency %s is not valid for market %s", currency, market));
        }

        this.items = List.copyOf(items);
        this.receivedAt = receivedAt != null ? receivedAt : Instant.now();
    }

    public void approve(OrderTotals totals, Instant processedAt) {
        this.status = OrderStatus.APPROVED;
        this.totals = totals;
        this.rejectionReason = null;
        this.processedAt = processedAt;
    }

    public void reject(String reason, Instant processedAt) {
        this.status = OrderStatus.REJECTED;
        this.rejectionReason = reason;
        this.totals = OrderTotals.zero();
        this.processedAt = processedAt;
    }

    public void markTechnicalFailure(String reason, Instant processedAt) {
        this.status = OrderStatus.TECHNICAL_FAILURE;
        this.rejectionReason = reason;
        this.processedAt = processedAt;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getEventId() {
        return eventId;
    }

    public long getEventVersion() {
        return eventVersion;
    }

    public Market getMarket() {
        return market;
    }

    public Currency getCurrency() {
        return currency;
    }

    public String getClientId() {
        return clientId;
    }

    public List<OrderLine> getItems() {
        return items;
    }

    public OrderStatus getStatus() {
        return status;
    }

    public OrderTotals getTotals() {
        return totals;
    }

    public String getRejectionReason() {
        return rejectionReason;
    }

    public Instant getReceivedAt() {
        return receivedAt;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }
}
