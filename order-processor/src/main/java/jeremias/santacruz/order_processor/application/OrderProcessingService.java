package jeremias.santacruz.order_processor.application;

import jeremias.santacruz.order_processor.domain.model.client.Client;
import jeremias.santacruz.order_processor.domain.model.order.Currency;
import jeremias.santacruz.order_processor.domain.model.order.Market;
import jeremias.santacruz.order_processor.domain.model.order.Order;
import jeremias.santacruz.order_processor.domain.model.order.OrderLine;
import jeremias.santacruz.order_processor.domain.model.order.OrderTotals;
import jeremias.santacruz.order_processor.domain.model.product.Product;
import jeremias.santacruz.order_processor.domain.port.in.ProcessOrderUseCase;
import jeremias.santacruz.order_processor.domain.port.out.ClientsClientPort;
import jeremias.santacruz.order_processor.domain.port.out.EventPublisherPort;
import jeremias.santacruz.order_processor.domain.port.out.ExternalServiceException;
import jeremias.santacruz.order_processor.domain.port.out.OrderRepositoryPort;
import jeremias.santacruz.order_processor.domain.port.out.ProductsClientPort;
import jeremias.santacruz.order_processor.domain.service.OrderCalculatorService;
import jeremias.santacruz.order_processor.domain.service.OrderCalculatorService.CalculatedLine;
import jeremias.santacruz.order_processor.domain.service.OrderEligibilityService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Implementación del caso de uso de procesamiento de pedidos: orquesta los puertos de salida
 * y los servicios de dominio para llevar un evento {@code orders.created.v1} a su estado final.
 *
 * <p>Flujo (secciones 6 y 7 de Especificaciones.md):</p>
 * <ol>
 *   <li>Idempotencia: una re-entrega del mismo {@code eventId} no duplica efectos (7.1).</li>
 *   <li>Control de versión por {@code orderId}: un evento obsoleto se ignora (7.3); dos
 *       versiones concurrentes se resuelven con la operación atómica del repositorio (7.2).</li>
 *   <li>Consulta de contexto externo: cliente (404 = {@code REJECTED}) y productos, con la
 *       matriz de errores de la sección 7 (transitorios → reintento con backoff, definitivos →
 *       resultado {@code TECHNICAL_FAILURE}).</li>
 *   <li>Elegibilidad (6.1) y cálculo financiero con {@code BigDecimal}/{@code HALF_UP} (6.2-6.4)
 *       delegados en los servicios de dominio.</li>
 *   <li>Persistencia del resultado y publicación de {@code orders.processed.v1} (5.D).</li>
 * </ol>
 *
 * <p>Esta capa no conoce Kafka, JSON ni HTTP: solo puertos del dominio.</p>
 */
@Service
public class OrderProcessingService implements ProcessOrderUseCase {

    private static final Logger log = LoggerFactory.getLogger(OrderProcessingService.class);

    private final ClientsClientPort clientsClientPort;
    private final ProductsClientPort productsClientPort;
    private final OrderRepositoryPort orderRepositoryPort;
    private final EventPublisherPort eventPublisherPort;

    /** Reglas de elegibilidad (6.1): lógica pura de dominio. */
    private final OrderEligibilityService eligibility = new OrderEligibilityService();

    /** Reglas de cálculo financiero y de impuestos (6.2-6.4): lógica pura de dominio. */
    private final OrderCalculatorService calculator = new OrderCalculatorService();

    public OrderProcessingService(ClientsClientPort clientsClientPort,
                                  ProductsClientPort productsClientPort,
                                  OrderRepositoryPort orderRepositoryPort,
                                  EventPublisherPort eventPublisherPort) {
        this.clientsClientPort = clientsClientPort;
        this.productsClientPort = productsClientPort;
        this.orderRepositoryPort = orderRepositoryPort;
        this.eventPublisherPort = eventPublisherPort;
    }

    @Override
    public Order processOrder(ProcessOrderCommand command) {
        Objects.requireNonNull(command, "command cannot be null");

        // 7.1: re-entrega del mismo eventId → no duplica efectos ni cambia el resultado guardado.
        if (orderRepositoryPort.existsByEventId(command.eventId())) {
            Order existing = orderRepositoryPort.findByOrderId(command.orderId()).orElseThrow(() ->
                    new IllegalStateException("eventId=" + command.eventId()
                            + " existe pero no se encontró el pedido orderId=" + command.orderId()));
            log.info("eventId={} ya procesado; se omite el recálculo (idempotencia)", command.eventId());
            // Si el resultado vigente es el de ESTE evento, se reenvía: si la publicación anterior
            // falló tras persistir, la re-entrega la sanja (at-least-once; el consumidor de salida
            // deduplica por sourceEventId). Si una versión más reciente lo sustituyó, no se publica.
            if (existing.getEventId().equals(command.eventId()) && existing.getStatus() != null) {
                eventPublisherPort.publishOrderProcessed(existing);
            }
            return existing;
        }

        // 7.3: versión antigua recibida después de una más reciente → se ignora el evento.
        Optional<Order> persisted = orderRepositoryPort.findByOrderId(command.orderId());
        if (persisted.isPresent() && persisted.get().getEventVersion() > command.eventVersion()) {
            log.info("Pedido {} obsoleto: eventVersion {} < {}; se ignora el evento eventId={}",
                    command.orderId(), command.eventVersion(), persisted.get().getEventVersion(),
                    command.eventId());
            return persisted.get();
        }

        Order order = buildOrder(command);

        Optional<Client> client;
        Map<String, Product> products;
        try {
            client = clientsClientPort.getClient(command.clientId());
            products = client.isPresent() ? loadProducts(command) : Map.of();
        }
        catch (ExternalServiceException ex) {
            if (ex.isRetryable()) {
                // Transitorio (429/5xx/timeout): se propaga y el contenedor reintenta con backoff.
                throw ex;
            }
            // Definitivo: se persiste y publica el estado técnico para dejar constancia del fallo.
            return saveAndPublish(markTechnicalFailure(order, ex));
        }

        if (client.isEmpty()) {
            // Matriz de errores (7): 404 → recurso inexistente → estado REJECTED.
            order.reject("CLIENT_NOT_FOUND: Client " + command.clientId() + " does not exist", Instant.now());
            return saveAndPublish(order);
        }

        // 6.1: elegibilidad (cliente ACTIVE, market coincidente y productos ACTIVE).
        Optional<String> rejection = eligibility.evaluateEligibility(client.get(), products, order.getMarket());
        if (rejection.isPresent()) {
            order.reject(rejection.get(), Instant.now());
            return saveAndPublish(order);
        }

        // 6.2-6.4: cálculo financiero con BigDecimal y redondeo HALF_UP a 2 decimales.
        order.approve(calculateTotals(order, client.get(), products), Instant.now());
        return saveAndPublish(order);
    }

    /** Construye el agregado de dominio a partir del comando de entrada ya validado. */
    private Order buildOrder(ProcessOrderCommand command) {
        Market market = Market.valueOf(command.market());
        Currency currency = Currency.valueOf(command.currency());
        List<OrderLine> items = command.items().stream()
                .map(item -> new OrderLine(item.productId(), item.quantity(), item.unitPrice()))
                .toList();
        return new Order(command.orderId(), command.eventId(), command.eventVersion(), market, currency,
                command.clientId(), items, Instant.now());
    }

    /**
     * Consulta los productos del pedido. Las claves sin correspondencia se mapean a {@code null}
     * para que {@link OrderEligibilityService} las clasifique como {@code PRODUCT_NOT_FOUND}.
     */
    private Map<String, Product> loadProducts(ProcessOrderCommand command) {
        Set<String> productIds = new LinkedHashSet<>();
        for (OrderLineCommand item : command.items()) {
            productIds.add(item.productId());
        }
        Map<String, Product> found = productsClientPort.getProducts(productIds, command.market());
        Map<String, Product> products = new LinkedHashMap<>();
        for (String productId : productIds) {
            products.put(productId, found.get(productId));
        }
        return products;
    }

    /** Suma los importes ya redondeados de cada línea (6.4). */
    private OrderTotals calculateTotals(Order order, Client client, Map<String, Product> products) {
        List<CalculatedLine> lines = new ArrayList<>();
        for (OrderLine line : order.getItems()) {
            lines.add(calculator.calculateLine(line, client, products.get(line.productId()), order.getMarket()));
        }
        return calculator.calculateTotals(lines);
    }

    private Order markTechnicalFailure(Order order, ExternalServiceException ex) {
        String detail = ex.getStatusCode() != null ? "HTTP " + ex.getStatusCode() : "sin respuesta";
        String reason = "TECHNICAL_FAILURE: " + ex.getService() + " (" + detail + ") - " + ex.getMessage();
        log.error("Pedido {} no procesable de forma definitiva: {}", order.getOrderId(), reason);
        order.markTechnicalFailure(reason, Instant.now());
        return order;
    }

    /**
     * Persiste el resultado y publica el evento de salida.
     *
     * <p>Si el repositorio resolvió una carrera entre versiones concurrentes (7.2) y el estado
     * realmente persistido pertenece a otro evento, no se publica nada: el evento publicado debe
     * reflejar siempre lo que quedó guardado.</p>
     */
    private Order saveAndPublish(Order order) {
        Order stored = orderRepositoryPort.save(order);
        if (!order.getEventId().equals(stored.getEventId())) {
            log.warn("Pedido {} resuelto por la versión concurrente eventId={}; no se publica el resultado de eventId={}",
                    stored.getOrderId(), stored.getEventId(), order.getEventId());
            return stored;
        }
        eventPublisherPort.publishOrderProcessed(stored);
        return stored;
    }
}
