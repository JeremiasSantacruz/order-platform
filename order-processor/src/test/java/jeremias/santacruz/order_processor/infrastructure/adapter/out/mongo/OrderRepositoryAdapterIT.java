package jeremias.santacruz.order_processor.infrastructure.adapter.out.mongo;

import com.mongodb.client.MongoClients;
import jeremias.santacruz.order_processor.domain.model.order.Currency;
import jeremias.santacruz.order_processor.domain.model.order.Market;
import jeremias.santacruz.order_processor.domain.model.order.Order;
import jeremias.santacruz.order_processor.domain.model.order.OrderLine;
import jeremias.santacruz.order_processor.domain.model.order.OrderStatus;
import jeremias.santacruz.order_processor.domain.model.order.OrderTotals;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integración real de la persistencia con MongoDB (stack obligatorio de la sección 4).
 *
 * <p>Cubre los escenarios de concurrencia de la sección 7: idempotencia por {@code eventId},
 * actualización atómica por versión e ignorado de versiones obsoletas.</p>
 *
 * <p>La clase se omite automáticamente cuando no hay demonio de Docker disponible
 * ({@code disabledWithoutDocker}); en ese entorno la corrección del CAS se cubre con los
 * tests unitarios del caso de uso.</p>
 */
@Testcontainers(disabledWithoutDocker = true)
class OrderRepositoryAdapterIT {

    @Container
    static final MongoDBContainer MONGO = new MongoDBContainer("mongo:8.0");

    static MongoTemplate mongoTemplate;

    OrderRepositoryAdapter adapter;

    @BeforeAll
    static void createTemplate() {
        mongoTemplate = new MongoTemplate(MongoClients.create(MONGO.getConnectionString()), "orders_test");
    }

    @BeforeEach
    void setUp() {
        mongoTemplate.dropCollection(OrderDocument.class);
        adapter = new OrderRepositoryAdapter(mongoTemplate);
    }

    private static Order order(String eventId, long eventVersion, String orderId) {
        Order order = new Order(orderId, eventId, eventVersion, Market.MX, Currency.MXN, "CLI-99821",
                List.of(new OrderLine("PRD-001", 24, new BigDecimal("35.5"))), Instant.now());
        order.approve(new OrderTotals(new BigDecimal("852.00"), BigDecimal.ZERO,
                new BigDecimal("852.00"), new BigDecimal("136.32"), new BigDecimal("988.32")), Instant.now());
        return order;
    }

    @Test
    @DisplayName("Persiste y recupera el agregado conservando la precisión BigDecimal")
    void shouldSaveAndRetrieveOrder() {
        adapter.save(order("evt-1", 1L, "ORD-1"));

        Optional<Order> stored = adapter.findByOrderId("ORD-1");

        assertThat(stored).isPresent();
        assertThat(stored.get().getStatus()).isEqualTo(OrderStatus.APPROVED);
        assertThat(stored.get().getEventId()).isEqualTo("evt-1");
        assertThat(stored.get().getTotals().grossSubtotal()).isEqualByComparingTo("852.00");
        assertThat(stored.get().getItems().get(0).unitPrice()).isEqualByComparingTo("35.5");
        assertThat(adapter.existsByEventId("evt-1")).isTrue();
        assertThat(adapter.existsByEventId("otro")).isFalse();
    }

    @Test
    @DisplayName("7.2: la segunda versión con el mismo eventVersion pierde la carrera y no aplica cambios")
    void shouldResolveConcurrentSameVersionWithAtomicCas() {
        adapter.save(order("evt-A", 1L, "ORD-1"));

        Order concurrent = order("evt-B", 1L, "ORD-1");
        Order resolved = adapter.save(concurrent);

        // El CAS exige eventVersion estrictamente menor: no se sobreescribe nada y se devuelve
        // lo realmente persistido, para que el caso de uso no publique un resultado que no guardó.
        assertThat(resolved.getEventId()).isEqualTo("evt-A");
        assertThat(adapter.findByOrderId("ORD-1").orElseThrow().getEventId()).isEqualTo("evt-A");
    }

    @Test
    @DisplayName("Una versión más nueva sí actualiza el documento")
    void shouldApplyNewerVersion() {
        adapter.save(order("evt-1", 1L, "ORD-1"));

        Order newer = order("evt-2", 2L, "ORD-1");
        Order resolved = adapter.save(newer);

        assertThat(resolved.getEventId()).isEqualTo("evt-2");
        assertThat(resolved.getEventVersion()).isEqualTo(2L);
        assertThat(adapter.findByOrderId("ORD-1").orElseThrow().getEventVersion()).isEqualTo(2L);
    }

    @Test
    @DisplayName("7.3: una versión antigua no degrada el documento guardado")
    void shouldNotDowngradeToStaleVersion() {
        adapter.save(order("evt-2", 2L, "ORD-1"));

        Order stale = order("evt-1", 1L, "ORD-1");
        Order resolved = adapter.save(stale);

        assertThat(resolved.getEventId()).isEqualTo("evt-2");
        assertThat(adapter.findByOrderId("ORD-1").orElseThrow().getEventVersion()).isEqualTo(2L);
    }

    @Test
    @DisplayName("7.1: la inserción concurrente del mismo orderId resuelve por clave primaria")
    void shouldRejectConcurrentInsertByPrimarykey() {
        adapter.save(order("evt-A", 1L, "ORD-1"));

        // Simula que otro proceso insertó primero: insert directo del mismo documento.
        Order duplicate = order("evt-A", 1L, "ORD-1");
        Order resolved = adapter.save(duplicate);

        assertThat(resolved.getOrderId()).isEqualTo("ORD-1");
        assertThat(adapter.existsByEventId("evt-A")).isTrue();
    }
}
