package jeremias.santacruz.order_processor.infrastructure.adapter.in.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jeremias.santacruz.order_processor.domain.model.client.Client;
import jeremias.santacruz.order_processor.domain.model.client.ClientSegment;
import jeremias.santacruz.order_processor.domain.model.client.ClientStatus;
import jeremias.santacruz.order_processor.domain.model.client.TaxRegime;
import jeremias.santacruz.order_processor.domain.model.order.Market;
import jeremias.santacruz.order_processor.domain.model.order.Order;
import jeremias.santacruz.order_processor.domain.model.order.OrderStatus;
import jeremias.santacruz.order_processor.domain.model.product.Product;
import jeremias.santacruz.order_processor.domain.model.product.ProductStatus;
import jeremias.santacruz.order_processor.domain.model.product.TaxCategory;
import jeremias.santacruz.order_processor.domain.port.out.ClientsClientPort;
import jeremias.santacruz.order_processor.domain.port.out.OrderRepositoryPort;
import jeremias.santacruz.order_processor.domain.port.out.ProductsClientPort;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/**
 * Flujo completo de la interfaz de entrada: produce un evento en {@code orders.created.v1},
 * el listener lo procesa contra MongoDB (contenedor de prueba) y el resultado se consume de
 * {@code orders.processed.v1}. Todo mensaje termina publicado y persistido: no existe DLT.
 *
 * <p>Se omiten los tests si no hay demonio de Docker disponible.</p>
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest
@EmbeddedKafka(partitions = 1,
        topics = {OrderCreatedKafkaFlowIT.CREATED_TOPIC, OrderCreatedKafkaFlowIT.PROCESSED_TOPIC},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class OrderCreatedKafkaFlowIT {

    static final String CREATED_TOPIC = "orders.created.v1";
    static final String PROCESSED_TOPIC = "orders.processed.v1";

    private static final String EVENT_ID = "01J8ZP6M5E4RH0K7Y2N9A3TQWX";
    private static final String ORDER_ID = "ORD-MX-000147";

    @Container
    static final MongoDBContainer MONGO = new MongoDBContainer("mongo:8.0");

    @DynamicPropertySource
    static void mongoUri(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", () -> MONGO.getConnectionString() + "/orders_db");
    }

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private OrderRepositoryPort orderRepositoryPort;

    @MockitoBean
    private ClientsClientPort clientsClientPort;

    @MockitoBean
    private ProductsClientPort productsClientPort;

    @Test
    @DisplayName("5.A → 5.D: produce orders.created.v1 y consume el resultado APPROVED con los totales de la sección 6")
    void produceCreatedEventAndConsumeApprovedResult() throws Exception {
        when(clientsClientPort.getClient("CLI-99821")).thenReturn(Optional.of(new Client(
                "CLI-99821", "Distribuidora Confiable", ClientStatus.ACTIVE,
                ClientSegment.WHOLESALE, TaxRegime.GENERAL, Market.MX)));
        when(productsClientPort.getProducts(any(), eq("MX"))).thenReturn(Map.of(
                "PRD-001", new Product("PRD-001", "Teclado Mecanico", "SKU-001",
                        ProductStatus.ACTIVE, TaxCategory.STANDARD),
                "PRD-008", new Product("PRD-008", "Monitor 27", "SKU-008",
                        ProductStatus.ACTIVE, TaxCategory.STANDARD)));

        kafkaTemplate.send(CREATED_TOPIC, ORDER_ID, OrderEventFixtures.validPayload())
                .get(10, TimeUnit.SECONDS);

        ConsumerRecord<String, String> output =
                awaitRecord(PROCESSED_TOPIC, ORDER_ID, Duration.ofSeconds(30));

        JsonNode event = objectMapper.readTree(output.value());
        assertThat(event.path("eventId").asText()).isEqualTo(EVENT_ID + "-OUT");
        assertThat(event.path("sourceEventId").asText()).isEqualTo(EVENT_ID);
        assertThat(event.path("eventVersion").asLong()).isEqualTo(1L);
        assertThat(event.path("orderId").asText()).isEqualTo(ORDER_ID);
        assertThat(event.path("status").asText()).isEqualTo("APPROVED");
        assertThat(event.path("market").asText()).isEqualTo("MX");
        assertThat(event.path("currency").asText()).isEqualTo("MXN");
        assertThat(event.path("reason").isMissingNode() || event.path("reason").isNull()).isTrue();

        JsonNode totals = event.path("totals");
        assertThat(totals.path("grossSubtotal").decimalValue()).isEqualByComparingTo("1836.00");
        assertThat(totals.path("discount").decimalValue()).isEqualByComparingTo("25.56");
        assertThat(totals.path("netSubtotal").decimalValue()).isEqualByComparingTo("1810.44");
        assertThat(totals.path("tax").decimalValue()).isEqualByComparingTo("289.67");
        assertThat(totals.path("grandTotal").decimalValue()).isEqualByComparingTo("2100.11");

        Order stored = orderRepositoryPort.findByOrderId(ORDER_ID).orElseThrow();
        assertThat(stored.getStatus()).isEqualTo(OrderStatus.APPROVED);
        assertThat(stored.getEventId()).isEqualTo(EVENT_ID);
        assertThat(stored.getItems()).hasSize(2);
        assertThat(stored.getTotals().grossSubtotal()).isEqualByComparingTo("1836.00");
        assertThat(stored.getTotals().grandTotal()).isEqualByComparingTo("2100.11");
    }

    @Test
    @DisplayName("5.A: un payload fuera de contrato se publica REJECTED en orders.processed.v1 (sin DLT)")
    void contractViolationIsRejectedOnProcessedTopic() throws Exception {
        String eventId = EVENT_ID + "-REJ";
        String orderId = "ORD-MX-000148";
        String payload = OrderEventFixtures.emptyItemsPayload()
                .replace(EVENT_ID, eventId)
                .replace(ORDER_ID, orderId);

        kafkaTemplate.send(CREATED_TOPIC, orderId, payload).get(10, TimeUnit.SECONDS);

        ConsumerRecord<String, String> output = awaitRecord(PROCESSED_TOPIC, orderId, Duration.ofSeconds(30));

        JsonNode event = objectMapper.readTree(output.value());
        assertThat(event.path("orderId").asText()).isEqualTo(orderId);
        assertThat(event.path("sourceEventId").asText()).isEqualTo(eventId);
        assertThat(event.path("status").asText()).isEqualTo("REJECTED");
        assertThat(event.path("reason").asText()).contains("CONTRACT_VIOLATION");

        Order stored = orderRepositoryPort.findByOrderId(orderId).orElseThrow();
        assertThat(stored.getStatus()).isEqualTo(OrderStatus.REJECTED);
        assertThat(stored.getRejectionReason()).contains("CONTRACT_VIOLATION");
    }

    @Test
    @DisplayName("7: un payload que ni siquiera es JSON se registra como no procesable en orders.processed.v1")
    void unparseablePayloadIsRejectedWithSalvagedIds() throws Exception {
        String orderId = "ORD-MX-000149";
        String payload = "{esto-no-es-json";

        kafkaTemplate.send(CREATED_TOPIC, orderId, payload).get(10, TimeUnit.SECONDS);

        ConsumerRecord<String, String> output = awaitRecord(PROCESSED_TOPIC, orderId, Duration.ofSeconds(30));

        JsonNode event = objectMapper.readTree(output.value());
        assertThat(event.path("orderId").asText()).isEqualTo(orderId);
        assertThat(event.path("status").asText()).isEqualTo("REJECTED");
        assertThat(event.path("reason").asText()).contains("DESERIALIZATION");
        assertThat(event.path("market").isNull()).isTrue();

        Order stored = orderRepositoryPort.findByOrderId(orderId).orElseThrow();
        assertThat(stored.getStatus()).isEqualTo(OrderStatus.REJECTED);
        assertThat(stored.getRejectionReason()).contains("DESERIALIZATION");
        assertThat(stored.getSourcePayload()).isEqualTo(payload);
    }

    private ConsumerRecord<String, String> awaitRecord(String topic, String key, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        try (KafkaConsumer<String, String> consumer = newConsumer()) {
            consumer.subscribe(List.of(topic));
            while (System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (key.equals(record.key())) {
                        return record;
                    }
                }
            }
        }
        return fail("No se recibio ningun mensaje con key='" + key + "' en '" + topic
                + "' dentro de " + timeout);
    }

    private KafkaConsumer<String, String> newConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "flow-it-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(properties);
    }
}