package jeremias.santacruz.order_processor.domain.model.order;

import jeremias.santacruz.order_processor.domain.model.client.Client;

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
    private Client client;
    private List<EnrichedOrderLine> enrichedLines;
    private String sourcePayload;

    public Order(String orderId, String eventId, long eventVersion, Market market, Currency currency,
                 String clientId, List<OrderLine> items, Instant receivedAt) {
        this(orderId, eventId, eventVersion, market, currency, clientId, items, receivedAt, true);
    }

    private Order(String orderId, String eventId, long eventVersion, Market market, Currency currency,
                  String clientId, List<OrderLine> items, Instant receivedAt, boolean strict) {
        this.orderId = Objects.requireNonNull(orderId, "orderId cannot be null");
        this.eventId = Objects.requireNonNull(eventId, "eventId cannot be null");
        this.eventVersion = eventVersion;

        if (strict) {
            this.market = Objects.requireNonNull(market, "market cannot be null");
            this.currency = Objects.requireNonNull(currency, "currency cannot be null");
            this.clientId = Objects.requireNonNull(clientId, "clientId cannot be null");
            if (items == null || items.isEmpty()) {
                throw new IllegalArgumentException("Order must contain at least one item");
            }
            if (!market.supportsCurrency(currency)) {
                throw new IllegalArgumentException(
                        String.format("Currency %s is not valid for market %s", currency, market));
            }
        }
        else {
            // Esqueleto de un pedido que el contrato de entrada no permitió completar: los campos
            // que faltan se conservan como desconocidos (null) en lugar de romper la construcción.
            this.market = market;
            this.currency = currency;
            this.clientId = clientId;
        }

        this.items = items == null ? List.of() : List.copyOf(items);
        this.receivedAt = receivedAt != null ? receivedAt : Instant.now();
    }

    /**
     * Reconstruye un pedido que no se pudo construir con el constructor estricto (payload con
     * campos obligatorios ausentes, mercado inválido o sin líneas). Solo crea el esqueleto: el
     * estado final se aplica con {@link #reject} o {@link #markTechnicalFailure}.
     *
     * <p>La persistencia usa este camino para documentos que llegaron a la colección con campos
     * parciales, de modo que leerlos no rompe el mapeo al agregado.</p>
     *
     * @param orderId      Identificador de negocio (o uno sintético cuando no venía en el payload)
     * @param eventId      Evento de entrada (o uno sintético)
     * @param eventVersion Versión del evento recibido, o 0 cuando no pudo leerse
     * @param market       Mercado si era válido, {@code null} si no
     * @param currency     Moneda si era válida, {@code null} si no
     * @param clientId     Cliente si venía en el payload, {@code null} si no
     * @param items        Líneas que sí se pudieron leer (puede ser vacío)
     * @param receivedAt   Momento de recepción
     */
    public static Order partial(String orderId, String eventId, long eventVersion, Market market,
                                Currency currency, String clientId, List<OrderLine> items,
                                Instant receivedAt) {
        return new Order(orderId, eventId, eventVersion, market, currency, clientId, items,
                receivedAt, false);
    }

    public void approve(OrderTotals totals, List<EnrichedOrderLine> enrichedLines, Client client,
                        Instant processedAt) {
        this.status = OrderStatus.APPROVED;
        this.totals = totals;
        this.enrichedLines = enrichedLines == null ? null : List.copyOf(enrichedLines);
        this.client = client;
        this.rejectionReason = null;
        this.processedAt = processedAt;
    }

    public void reject(String reason, Instant processedAt) {
        reject(reason, null, processedAt);
    }

    /**
     * Marca el pedido como rechazado conservando el snapshot del cliente cuando se conoce.
     *
     * @param reason      Razón explícita del rechazo
     * @param client      Cliente consultado, o {@code null} si aún no existía (404, contrato)
     * @param processedAt Momento de procesamiento
     */
    public void reject(String reason, Client client, Instant processedAt) {
        this.status = OrderStatus.REJECTED;
        this.rejectionReason = reason;
        this.client = client;
        this.enrichedLines = null;
        this.totals = OrderTotals.zero();
        this.processedAt = processedAt;
    }

    public void markTechnicalFailure(String reason, Instant processedAt) {
        markTechnicalFailure(reason, null, processedAt);
    }

    /**
     * Marca el pedido como fallo técnico conservando el snapshot del cliente cuando se conoce.
     *
     * @param reason      Razón del fallo
     * @param client      Cliente consultado, o {@code null} si no llegó a resolverse
     * @param processedAt Momento de procesamiento
     */
    public void markTechnicalFailure(String reason, Client client, Instant processedAt) {
        this.status = OrderStatus.TECHNICAL_FAILURE;
        this.rejectionReason = reason;
        this.client = client;
        this.enrichedLines = null;
        this.totals = OrderTotals.zero();
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

    /** Snapshot del cliente al momento del procesamiento; {@code null} si nunca se resolvió. */
    public Client getClient() {
        return client;
    }

    /** Líneas enriquecidas; solo se llenan cuando el pedido fue aprobado (se calculó). */
    public List<EnrichedOrderLine> getEnrichedLines() {
        return enrichedLines;
    }

    /**
     * Payload crudo del mensaje de entrada, conservado como trazabilidad.
     *
     * <p>Solo se llena cuando el mensaje no pudo procesarse: en ese caso ni la razón ni los campos
     * del documento alcanzan para reproducir el incidente sin volver al broker.</p>
     */
    public void setSourcePayload(String sourcePayload) {
        this.sourcePayload = sourcePayload;
    }

    public String getSourcePayload() {
        return sourcePayload;
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
