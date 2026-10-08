package jeremias.santacruz.order_processor.application;

import jeremias.santacruz.order_processor.domain.model.client.Client;
import jeremias.santacruz.order_processor.domain.model.order.Currency;
import jeremias.santacruz.order_processor.domain.model.order.EnrichedOrderLine;
import jeremias.santacruz.order_processor.domain.model.order.Market;
import jeremias.santacruz.order_processor.domain.model.order.Order;
import jeremias.santacruz.order_processor.domain.model.order.OrderLine;
import jeremias.santacruz.order_processor.domain.model.product.Product;
import jeremias.santacruz.order_processor.domain.port.in.ProcessOrderUseCase;
import jeremias.santacruz.order_processor.domain.port.out.ClientsClientPort;
import jeremias.santacruz.order_processor.domain.port.out.EventPublisherPort;
import jeremias.santacruz.order_processor.domain.port.out.ExternalServiceException;
import jeremias.santacruz.order_processor.domain.port.out.OrderRepositoryPort;
import jeremias.santacruz.order_processor.domain.port.out.ProductsClientPort;
import jeremias.santacruz.order_processor.domain.service.OrderCalculatorService;
import jeremias.santacruz.order_processor.domain.service.OrderContractService;
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
import java.util.UUID;

/**
 * Implementación del caso de uso de procesamiento de pedidos: orquesta los puertos de salida
 * y los servicios de dominio para llevar un evento {@code orders.created.v1} a su estado final.
 *
 * <p>Flujo:</p>
 * <ol>
 *   <li>Idempotencia: una re-entrega del mismo {@code eventId} no duplica efectos.</li>
 *   <li>Control de versión por {@code orderId}: un evento obsoleto se ignora; dos versiones
 *       concurrentes se resuelven con la operación atómica del repositorio.</li>
 *   <li>Validación del contrato de entrada: una violación es un resultado de negocio, no un fallo
 *       del mensaje, así que se registra como {@code REJECTED} con la razón y se persiste.</li>
 *   <li>Consulta de contexto externo: cliente (404 = {@code REJECTED}) y productos, con la
 *       matriz de errores (transitorios → reintento con backoff, definitivos → resultado
 *       {@code TECHNICAL_FAILURE}).</li>
 *   <li>Elegibilidad y cálculo financiero con {@code BigDecimal}/{@code HALF_UP} delegados en los
 *       servicios de dominio.</li>
 *   <li>Persistencia del resultado y publicación de {@code orders.processed.v1}.</li>
 * </ol>
 *
 * <p>Cada mensaje que llega a este caso de uso termina en uno de los tres estados permitidos y
 * queda guardado en la base de datos: no existe un destino paralelo para mensajes fallidos.</p>
 *
 * <p>Esta capa no conoce Kafka, JSON ni HTTP: solo puertos del dominio.</p>
 */
@Service
public class OrderProcessingService implements ProcessOrderUseCase {

    private static final Logger log = LoggerFactory.getLogger(OrderProcessingService.class);

    /** Prefijo de la razón cuando el fallo viene del contrato de entrada. */
    private static final String CONTRACT_VIOLATION_PREFIX = "CONTRACT_VIOLATION: ";

    /** Prefijo de la razón cuando el fallo viene de un error externo o de reintentos agotados. */
    private static final String TECHNICAL_FAILURE_PREFIX = "TECHNICAL_FAILURE: ";

    /** Sintético para orderId ausente: sin identificador no hay clave primaria en la base. */
    private static final String SYNTHETIC_ORDER_PREFIX = "UNPARSEABLE-";

    private final ClientsClientPort clientsClientPort;
    private final ProductsClientPort productsClientPort;
    private final OrderRepositoryPort orderRepositoryPort;
    private final EventPublisherPort eventPublisherPort;

    /** Reglas del contrato de entrada: lógica pura de dominio. */
    private final OrderContractService contract = new OrderContractService();

    /** Reglas de elegibilidad: lógica pura de dominio. */
    private final OrderEligibilityService eligibility = new OrderEligibilityService();

    /** Reglas de cálculo financiero y de impuestos: lógica pura de dominio. */
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

        // Idempotencia: re-entrega del mismo eventId → no duplica efectos ni cambia el resultado.
        // Versión antigua (eventVersion menor que la persistida) → se ignora el evento.
        Optional<Order> existing = resolveExisting(command);
        if (existing.isPresent()) {
            return existing.get();
        }

        // Contrato de entrada: violación → resultado de negocio, no fallo del mensaje.
        List<String> violations = contract.validate(command);
        if (!violations.isEmpty()) {
            return saveAndPublish(contractViolation(command, violations));
        }

        Order order = buildOrder(command);

        Optional<Client> client = Optional.empty();
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
            return saveAndPublish(markTechnicalFailure(order, ex, client.orElse(null)));
        }

        if (client.isEmpty()) {
            // Matriz de errores: 404 → recurso inexistente → estado REJECTED.
            order.reject("CLIENT_NOT_FOUND: Client " + command.clientId() + " does not exist", Instant.now());
            return saveAndPublish(order);
        }

        Optional<String> rejection = eligibility.evaluateEligibility(client.get(), products, order.getMarket());
        if (rejection.isPresent()) {
            order.reject(rejection.get(), client.get(), Instant.now());
            return saveAndPublish(order);
        }

        // Cálculo financiero con BigDecimal y redondeo HALF_UP a 2 decimales.
        List<EnrichedOrderLine> lines = enrichLines(order, client.get(), products);
        order.approve(calculator.calculateTotals(lines), lines, client.get(), Instant.now());
        return saveAndPublish(order);
    }

    @Override
    public Order recordUnprocessable(UnprocessableCommand command) {
        Objects.requireNonNull(command, "command cannot be null");

        String orderId = isBlank(command.orderId())
                ? SYNTHETIC_ORDER_PREFIX + UUID.randomUUID()
                : command.orderId();
        String eventId = isBlank(command.eventId())
                ? SYNTHETIC_ORDER_PREFIX + orderId
                : command.eventId();

        Order order = Order.partial(orderId, eventId, 0L,
                OrderContractService.parseMarket(command.market()),
                OrderContractService.parseCurrency(command.currency()),
                isBlank(command.clientId()) ? null : command.clientId(),
                List.of(), Instant.now());
        order.setSourcePayload(command.sourcePayload());
        order.reject(command.reason(), Instant.now());

        log.warn("Mensaje no procesable orderId={} eventId={}: {}", orderId, eventId, command.reason());
        return saveAndPublish(order);
    }

    @Override
    public Order recordTechnicalFailure(ProcessOrderCommand command, String detail) {
        Objects.requireNonNull(command, "command cannot be null");

        Optional<Order> existing = resolveExisting(command);
        if (existing.isPresent()) {
            return existing.get();
        }

        List<String> violations = contract.validate(command);
        if (!violations.isEmpty()) {
            return saveAndPublish(contractViolation(command, violations));
        }

        Order order = buildOrder(command);
        String reason = TECHNICAL_FAILURE_PREFIX + detail;
        log.error("Pedido {} no procesable de forma definitiva: {}", order.getOrderId(), reason);
        order.markTechnicalFailure(reason, Instant.now());
        return saveAndPublish(order);
    }

    /**
     * Resultado vigente si el mensaje ya no debe procesarse.
     *
     * <p>Devuelve el pedido guardado cuando (a) su {@code eventId} ya se procesó (reenviando el
     * resultado por si la publicación anterior falló) o (b) su versión es más reciente que la del
     * mensaje entrante.</p>
     */
    private Optional<Order> resolveExisting(ProcessOrderCommand command) {
        if (!isBlank(command.eventId()) && orderRepositoryPort.existsByEventId(command.eventId())) {
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
            return Optional.of(existing);
        }

        if (isBlank(command.orderId())) {
            return Optional.empty();
        }
        Optional<Order> persisted = orderRepositoryPort.findByOrderId(command.orderId());
        if (persisted.isPresent() && persisted.get().getEventVersion() > command.eventVersion()) {
            log.info("Pedido {} obsoleto: eventVersion {} < {}; se ignora el evento eventId={}",
                    command.orderId(), command.eventVersion(), persisted.get().getEventVersion(),
                    command.eventId());
            return persisted;
        }
        return Optional.empty();
    }

    /**
     * Convierte una violación del contrato de entrada en un pedido {@code REJECTED} persistible.
     *
     * <p>Si el agregado pudo construirse completo (la violación fue una regla "blanda": producto
     * duplicado, cantidad o precio) se conservan mercado, moneda, cliente y líneas en el
     * documento; si la violación impidió construirlo (campo obligatorio ausente, mercado inválido
     * o sin líneas) se arma un esqueleto con los campos que sí se pudieron leer.</p>
     */
    private Order contractViolation(ProcessOrderCommand command, List<String> violations) {
        String reason = CONTRACT_VIOLATION_PREFIX + String.join("; ", violations);
        Order order = canBuildOrder(command) ? buildOrder(command) : partialOf(command);
        log.warn("Pedido {} rechazado por contrato de entrada: {}", order.getOrderId(), reason);
        order.reject(reason, Instant.now());
        return order;
    }

    /** True cuando el comando permite construir el agregado con el constructor estricto. */
    private boolean canBuildOrder(ProcessOrderCommand command) {
        if (isBlank(command.orderId()) || isBlank(command.eventId()) || isBlank(command.clientId())) {
            return false;
        }
        if (command.items() == null || command.items().isEmpty()
                || command.items().stream().anyMatch(Objects::isNull)) {
            return false;
        }
        Market market = OrderContractService.parseMarket(command.market());
        Currency currency = OrderContractService.parseCurrency(command.currency());
        return market != null && currency != null && market.supportsCurrency(currency);
    }

    /** Esqueleto del pedido con lo que sí se pudo leer del comando (identificadores y campos). */
    private Order partialOf(ProcessOrderCommand command) {
        return Order.partial(resolveOrderId(command), resolveEventId(command), command.eventVersion(),
                OrderContractService.parseMarket(command.market()),
                OrderContractService.parseCurrency(command.currency()),
                isBlank(command.clientId()) ? null : command.clientId(),
                toOrderLines(command.items()), Instant.now());
    }

    /**
     * orderId del comando; si el mensaje no lo traía se deriva del eventId (y en el peor de los
     * casos, de un UUID) porque sin identificador no hay clave primaria en la base.
     */
    private static String resolveOrderId(ProcessOrderCommand command) {
        if (!isBlank(command.orderId())) {
            return command.orderId();
        }
        if (!isBlank(command.eventId())) {
            return SYNTHETIC_ORDER_PREFIX + command.eventId();
        }
        return SYNTHETIC_ORDER_PREFIX + UUID.randomUUID();
    }

    /** eventId del comando, o un sintético cuando el mensaje no lo traía. */
    private static String resolveEventId(ProcessOrderCommand command) {
        return isBlank(command.eventId()) ? SYNTHETIC_ORDER_PREFIX + UUID.randomUUID() : command.eventId();
    }

    /** Construye el agregado de dominio a partir del comando de entrada ya validado. */
    private Order buildOrder(ProcessOrderCommand command) {
        Market market = Market.valueOf(command.market());
        Currency currency = Currency.valueOf(command.currency());
        return new Order(command.orderId(), command.eventId(), command.eventVersion(), market, currency,
                command.clientId(), toOrderLines(command.items()), Instant.now());
    }

    private static List<OrderLine> toOrderLines(List<OrderLineCommand> items) {
        if (items == null) {
            return List.of();
        }
        return items.stream()
                .filter(Objects::nonNull)
                .map(item -> new OrderLine(item.productId(), item.quantity(), item.unitPrice()))
                .toList();
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

    /** Líneas enriquecidas de cada renglón del pedido, ya con su contexto fiscal. */
    private List<EnrichedOrderLine> enrichLines(Order order, Client client, Map<String, Product> products) {
        List<EnrichedOrderLine> lines = new ArrayList<>();
        for (OrderLine line : order.getItems()) {
            lines.add(calculator.calculateLine(line, client, products.get(line.productId()),
                    order.getMarket()));
        }
        return lines;
    }

    private Order markTechnicalFailure(Order order, ExternalServiceException ex, Client client) {
        String detail = ex.getStatusCode() != null ? "HTTP " + ex.getStatusCode() : "sin respuesta";
        String reason = TECHNICAL_FAILURE_PREFIX + ex.getService() + " (" + detail + ") - "
                + ex.getMessage();
        log.error("Pedido {} no procesable de forma definitiva: {}", order.getOrderId(), reason);
        order.markTechnicalFailure(reason, client, Instant.now());
        return order;
    }

    /**
     * Persiste el resultado y publica el evento de salida.
     *
     * <p>Si el repositorio resolvió una carrera entre versiones concurrentes y el estado
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

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
