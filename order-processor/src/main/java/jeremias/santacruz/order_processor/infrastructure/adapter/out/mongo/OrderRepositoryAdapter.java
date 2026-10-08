package jeremias.santacruz.order_processor.infrastructure.adapter.out.mongo;

import jeremias.santacruz.order_processor.domain.model.order.Currency;
import jeremias.santacruz.order_processor.domain.model.order.Market;
import jeremias.santacruz.order_processor.domain.model.order.Order;
import jeremias.santacruz.order_processor.domain.model.order.OrderStatus;
import jeremias.santacruz.order_processor.domain.model.order.OrderTotals;
import jeremias.santacruz.order_processor.domain.port.out.OrderRepositoryPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

/**
 * Repositorio Mongo del agregado {@code Order}.
 *
 * <p>Garantiza los escenarios de concurrencia de la sección 7 con operaciones atómicas del
 * propio servidor MongoDB:</p>
 * <ul>
 *   <li><b>Inserción</b> del pedido nuevo: la clave primaria {@code orderId} convierte la carrera
 *       entre dos primeros eventos en {@link DuplicateKeyException}, que se resuelve con la
 *       actualización condicionada.</li>
 *   <li><b>Actualización por versión</b>: {@code findAndModify} con el filtro
 *       {@code eventVersion < <versión entrante>} es un compare-and-set atómico. Si otra versión
 *       igual o más reciente ganó, no se modifica nada y se devuelve el documento realmente
 *       persistido: el caso de uso detecta por el {@code eventId} que no debe publicar.</li>
 * </ul>
 *
 * <p>No se crean índices en arranque (evita conectarse a Mongo al levantar el contexto); el
 * índice {@code {eventId: 1}} es deuda técnica documentada en implementation-notes.</p>
 */
@Component
public class OrderRepositoryAdapter implements OrderRepositoryPort {

    private static final Logger log = LoggerFactory.getLogger(OrderRepositoryAdapter.class);

    private final MongoTemplate mongoTemplate;

    public OrderRepositoryAdapter(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public Optional<Order> findByOrderId(String orderId) {
        return Optional.ofNullable(mongoTemplate.findById(orderId, OrderDocument.class))
                .map(this::toDomain);
    }

    @Override
    public boolean existsByEventId(String eventId) {
        return mongoTemplate.exists(Query.query(Criteria.where("eventId").is(eventId)), OrderDocument.class);
    }

    @Override
    public Order save(Order order) {
        OrderDocument document = toDocument(order);

        // Inserción atómica de un pedido nuevo; si ya existe, es una carrera entre eventos.
        try {
            mongoTemplate.insert(document);
            return order;
        }
        catch (DuplicateKeyException race) {
            log.debug("Pedido {} ya existe (carrera de inserción); se intenta actualización por versión",
                    order.getOrderId());
        }

        // Compare-and-set atómico por versión (7.2): solo aplica si la versión guardada es menor.
        Query query = new Query(Criteria.where("_id").is(order.getOrderId())
                .and("eventVersion").lt(order.getEventVersion()));
        Update update = new Update()
                .set("eventId", document.eventId())
                .set("eventVersion", document.eventVersion())
                .set("market", document.market())
                .set("currency", document.currency())
                .set("clientId", document.clientId())
                .set("items", document.items())
                .set("status", document.status())
                .set("totals", document.totals())
                .set("rejectionReason", document.rejectionReason())
                .set("processedAt", document.processedAt())
                // Amplía el documento cuando una versión previa dejó campos sin resolver.
                .set("client", document.client())
                .set("enrichedLines", document.enrichedLines())
                .set("sourcePayload", document.sourcePayload());
        // receivedAt no se sobreescribe: conserva la primera recepción del pedido.

        OrderDocument updated = mongoTemplate.findAndModify(query, update,
                FindAndModifyOptions.options().returnNew(true), OrderDocument.class);
        if (updated != null) {
            return toDomain(updated);
        }

        // Otra versión (igual o más reciente) ganó la carrera: se devuelve lo persistido.
        OrderDocument stored = mongoTemplate.findById(order.getOrderId(), OrderDocument.class);
        if (stored == null) {
            throw new IllegalStateException(
                    "Conflicto de versión sin documento para el pedido " + order.getOrderId());
        }
        log.warn("Pedido {} resuelto por la versión concurrente eventId={}; no se aplica eventId={}",
                stored.orderId(), stored.eventId(), document.eventId());
        return toDomain(stored);
    }

    private OrderDocument toDocument(Order order) {
        return new OrderDocument(
                order.getOrderId(),
                order.getEventId(),
                order.getEventVersion(),
                // Un pedido parcial (contrato incumplido) puede no tener mercado/moneda/cliente.
                order.getMarket() == null ? null : order.getMarket().name(),
                order.getCurrency() == null ? null : order.getCurrency().name(),
                order.getClientId(),
                order.getItems(),
                order.getStatus() == null ? null : order.getStatus().name(),
                order.getTotals(),
                order.getRejectionReason(),
                order.getReceivedAt(),
                order.getProcessedAt(),
                order.getClient(),
                order.getEnrichedLines(),
                order.getSourcePayload());
    }

    private Order toDomain(OrderDocument document) {
        // Se reconstruye con la fábrica no estricta: un documento escrito por el camino de mensaje
        // no procesable puede traer market/currency/clientId nulos y sin líneas, y leerlo no debe
        // romper el mapeo (los campos ausentes se conservan como desconocidos).
        Order order = Order.partial(
                document.orderId(),
                document.eventId(),
                document.eventVersion(),
                document.market() == null ? null : Market.valueOf(document.market()),
                document.currency() == null ? null : Currency.valueOf(document.currency()),
                document.clientId(),
                document.items() == null ? List.of() : document.items(),
                document.receivedAt());

        if (document.status() == null) {
            return order; // documento a medias: no debería existir (solo se persiste con estado final)
        }
        switch (OrderStatus.valueOf(document.status())) {
            case APPROVED -> order.approve(document.totals() != null ? document.totals() : OrderTotals.zero(),
                    document.enrichedLines(), document.client(), document.processedAt());
            case REJECTED -> order.reject(document.rejectionReason(), document.client(),
                    document.processedAt());
            case TECHNICAL_FAILURE -> order.markTechnicalFailure(document.rejectionReason(),
                    document.client(), document.processedAt());
        }
        order.setSourcePayload(document.sourcePayload());
        return order;
    }
}
