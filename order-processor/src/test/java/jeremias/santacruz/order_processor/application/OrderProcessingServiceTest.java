package jeremias.santacruz.order_processor.application;

import jeremias.santacruz.order_processor.domain.model.client.Client;
import jeremias.santacruz.order_processor.domain.model.client.ClientSegment;
import jeremias.santacruz.order_processor.domain.model.client.ClientStatus;
import jeremias.santacruz.order_processor.domain.model.client.TaxRegime;
import jeremias.santacruz.order_processor.domain.model.order.Currency;
import jeremias.santacruz.order_processor.domain.model.order.Market;
import jeremias.santacruz.order_processor.domain.model.order.Order;
import jeremias.santacruz.order_processor.domain.model.order.OrderLine;
import jeremias.santacruz.order_processor.domain.model.order.OrderStatus;
import jeremias.santacruz.order_processor.domain.model.order.OrderTotals;
import jeremias.santacruz.order_processor.domain.model.product.Product;
import jeremias.santacruz.order_processor.domain.model.product.ProductStatus;
import jeremias.santacruz.order_processor.domain.model.product.TaxCategory;
import jeremias.santacruz.order_processor.domain.port.in.ProcessOrderUseCase;
import jeremias.santacruz.order_processor.domain.port.in.ProcessOrderUseCase.OrderLineCommand;
import jeremias.santacruz.order_processor.domain.port.in.ProcessOrderUseCase.ProcessOrderCommand;
import jeremias.santacruz.order_processor.domain.port.out.ClientsClientPort;
import jeremias.santacruz.order_processor.domain.port.out.EventPublisherPort;
import jeremias.santacruz.order_processor.domain.port.out.ExternalServiceException;
import jeremias.santacruz.order_processor.domain.port.out.OrderRepositoryPort;
import jeremias.santacruz.order_processor.domain.port.out.ProductsClientPort;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests del caso de uso: orquestación de puertos, reglas de elegibilidad/cálculo
 * e idempotencia/control de versión. Los servicios de dominio se usan reales porque
 * son lógica pura; solo se mockean los puertos de infraestructura.
 */
class OrderProcessingServiceTest {

    private static final String EVENT_ID = "01J8ZP6M5E4RH0K7Y2N9A3TQWX";
    private static final String ORDER_ID = "ORD-MX-000147";
    private static final String CLIENT_ID = "CLI-99821";

    private ClientsClientPort clientsClientPort;
    private ProductsClientPort productsClientPort;
    private OrderRepositoryPort orderRepositoryPort;
    private EventPublisherPort eventPublisherPort;
    private OrderProcessingService service;

    @BeforeEach
    void setUp() {
        clientsClientPort = mock(ClientsClientPort.class);
        productsClientPort = mock(ProductsClientPort.class);
        orderRepositoryPort = mock(OrderRepositoryPort.class);
        eventPublisherPort = mock(EventPublisherPort.class);
        service = new OrderProcessingService(clientsClientPort, productsClientPort, orderRepositoryPort,
                eventPublisherPort);
    }

    // ---------------------------------------------------------------- helpers

    /** Comando con los datos del ejemplo de la sección 5.A de la especificación. */
    private ProcessOrderCommand specCommand() {
        return new ProcessOrderCommand(EVENT_ID, 1L, ORDER_ID, "MX", "MXN", CLIENT_ID, "C1",
                List.of(new OrderLineCommand("PRD-001", 24, new BigDecimal("35.5")),
                        new OrderLineCommand("PRD-008", 12, new BigDecimal("82.0"))));
    }

    private Client activeWholesaleClient() {
        return new Client(CLIENT_ID, "Distribuidora Central", ClientStatus.ACTIVE, ClientSegment.WHOLESALE,
                TaxRegime.GENERAL, Market.MX);
    }

    private Product activeStandardProduct(String productId) {
        return new Product(productId, "Producto " + productId, "SKU-" + productId, ProductStatus.ACTIVE,
                TaxCategory.STANDARD);
    }

    private Order orderFixture(String eventId, long eventVersion) {
        return new Order(ORDER_ID, eventId, eventVersion, Market.MX, Currency.MXN, CLIENT_ID,
                List.of(new OrderLine("PRD-001", 1, BigDecimal.ONE)), null);
    }

    private void givenNoPriorActivity() {
        when(orderRepositoryPort.existsByEventId(anyString())).thenReturn(false);
        when(orderRepositoryPort.findByOrderId(anyString())).thenReturn(Optional.empty());
        when(orderRepositoryPort.save(any())).thenAnswer(invocation -> invocation.getArgument(0));
    }

    // ---------------------------------------------------------------- flujo feliz

    @Test
    @DisplayName("Aprueba, persiste y publica con los totales exactos del ejemplo de la sección 5.D")
    void shouldApproveAndPublishWithSpecTotals() {
        givenNoPriorActivity();
        when(clientsClientPort.getClient(CLIENT_ID)).thenReturn(Optional.of(activeWholesaleClient()));
        when(productsClientPort.getProducts(anySet(), anyString())).thenReturn(Map.of(
                "PRD-001", activeStandardProduct("PRD-001"),
                "PRD-008", activeStandardProduct("PRD-008")));

        Order result = service.processOrder(specCommand());

        assertThat(result.getStatus()).isEqualTo(OrderStatus.APPROVED);
        assertThat(result.getRejectionReason()).isNull();
        // Totales del evento de salida de la spec: 1836.00 / 25.56 / 1810.44 / 289.67 / 2100.11
        assertThat(result.getTotals().grossSubtotal()).isEqualByComparingTo("1836.00");
        assertThat(result.getTotals().discount()).isEqualByComparingTo("25.56");
        assertThat(result.getTotals().netSubtotal()).isEqualByComparingTo("1810.44");
        assertThat(result.getTotals().tax()).isEqualByComparingTo("289.67");
        assertThat(result.getTotals().grandTotal()).isEqualByComparingTo("2100.11");

        verify(orderRepositoryPort).save(any());
        verify(eventPublisherPort).publishOrderProcessed(result);
    }

    // ------------------------------------------------------- idempotencia (7)

    @Test
    @DisplayName("7.1: la re-entrega del mismo eventId no duplica efectos ni vuelve a consultar")
    void shouldSkipWhenEventIdAlreadyProcessed() {
        Order existing = orderFixture(EVENT_ID, 1L);
        existing.approve(OrderTotals.zero(), List.of(), null, Instant.now());
        when(orderRepositoryPort.existsByEventId(EVENT_ID)).thenReturn(true);
        when(orderRepositoryPort.findByOrderId(ORDER_ID)).thenReturn(Optional.of(existing));

        Order result = service.processOrder(specCommand());

        assertThat(result).isSameAs(existing);
        verify(clientsClientPort, never()).getClient(anyString());
        verify(productsClientPort, never()).getProducts(anySet(), anyString());
        verify(orderRepositoryPort, never()).save(any());
        // Reenvía el resultado vigente sin recalcular (sanja publicaciones fallidas; el
        // consumidor de salida deduplica por sourceEventId).
        verify(eventPublisherPort).publishOrderProcessed(existing);
    }

    @Test
    @DisplayName("7.3: ignora un evento con eventVersion antiguo tras una versión más reciente")
    void shouldIgnoreStaleEventVersion() {
        Order newer = orderFixture("event-posterior", 2L);
        when(orderRepositoryPort.existsByEventId(EVENT_ID)).thenReturn(false);
        when(orderRepositoryPort.findByOrderId(ORDER_ID)).thenReturn(Optional.of(newer));

        Order result = service.processOrder(specCommand());

        assertThat(result).isSameAs(newer);
        verify(clientsClientPort, never()).getClient(anyString());
        verify(orderRepositoryPort, never()).save(any());
        verify(eventPublisherPort, never()).publishOrderProcessed(any());
    }

    @Test
    @DisplayName("7.2: si otra versión concurrente ganó la persistencia, no publica el resultado")
    void shouldNotPublishWhenConcurrentVersionWon() {
        Order winner = orderFixture("event-concurrente", 1L);
        when(orderRepositoryPort.existsByEventId(EVENT_ID)).thenReturn(false);
        when(orderRepositoryPort.findByOrderId(ORDER_ID)).thenReturn(Optional.empty());
        when(clientsClientPort.getClient(CLIENT_ID)).thenReturn(Optional.of(activeWholesaleClient()));
        when(productsClientPort.getProducts(anySet(), anyString())).thenReturn(Map.of(
                "PRD-001", activeStandardProduct("PRD-001"),
                "PRD-008", activeStandardProduct("PRD-008")));
        when(orderRepositoryPort.save(any())).thenReturn(winner);

        Order result = service.processOrder(specCommand());

        assertThat(result).isSameAs(winner);
        verify(eventPublisherPort, never()).publishOrderProcessed(any());
    }

    // ----------------------------------------------------------- rechazos (6.1)

    @Test
    @DisplayName("404 de Clients API → REJECTED con razón CLIENT_NOT_FOUND")
    void shouldRejectWhenClientNotFound() {
        givenNoPriorActivity();
        when(clientsClientPort.getClient(CLIENT_ID)).thenReturn(Optional.empty());

        Order result = service.processOrder(specCommand());

        assertThat(result.getStatus()).isEqualTo(OrderStatus.REJECTED);
        assertThat(result.getRejectionReason()).contains("CLIENT_NOT_FOUND").contains(CLIENT_ID);
        assertThat(result.getTotals().grandTotal()).isEqualByComparingTo("0");
        verify(productsClientPort, never()).getProducts(anySet(), anyString());
        verify(eventPublisherPort).publishOrderProcessed(result);
    }

    @Test
    @DisplayName("6.1: rechaza cuando el cliente no está ACTIVE")
    void shouldRejectWhenClientIsBlocked() {
        givenNoPriorActivity();
        Client blocked = new Client(CLIENT_ID, "Bloqueado", ClientStatus.BLOCKED, ClientSegment.WHOLESALE,
                TaxRegime.GENERAL, Market.MX);
        when(clientsClientPort.getClient(CLIENT_ID)).thenReturn(Optional.of(blocked));
        when(productsClientPort.getProducts(anySet(), anyString())).thenReturn(Map.of(
                "PRD-001", activeStandardProduct("PRD-001"),
                "PRD-008", activeStandardProduct("PRD-008")));

        Order result = service.processOrder(specCommand());

        assertThat(result.getStatus()).isEqualTo(OrderStatus.REJECTED);
        assertThat(result.getRejectionReason()).contains("CLIENT_INACTIVE");
        verify(eventPublisherPort).publishOrderProcessed(result);
    }

    @Test
    @DisplayName("6.1: rechaza cuando el market del cliente no coincide con el del pedido")
    void shouldRejectOnMarketMismatch() {
        givenNoPriorActivity();
        Client coClient = new Client(CLIENT_ID, "Cliente CO", ClientStatus.ACTIVE, ClientSegment.WHOLESALE,
                TaxRegime.GENERAL, Market.CO);
        when(clientsClientPort.getClient(CLIENT_ID)).thenReturn(Optional.of(coClient));
        when(productsClientPort.getProducts(anySet(), anyString())).thenReturn(Map.of(
                "PRD-001", activeStandardProduct("PRD-001"),
                "PRD-008", activeStandardProduct("PRD-008")));

        Order result = service.processOrder(specCommand());

        assertThat(result.getStatus()).isEqualTo(OrderStatus.REJECTED);
        assertThat(result.getRejectionReason()).contains("MARKET_MISMATCH");
    }

    @Test
    @DisplayName("6.1: rechaza cuando falta un producto (404 → PRODUCT_NOT_FOUND)")
    void shouldRejectWhenProductMissing() {
        givenNoPriorActivity();
        when(clientsClientPort.getClient(CLIENT_ID)).thenReturn(Optional.of(activeWholesaleClient()));
        when(productsClientPort.getProducts(anySet(), anyString()))
                .thenReturn(Map.of("PRD-001", activeStandardProduct("PRD-001")));

        Order result = service.processOrder(specCommand());

        assertThat(result.getStatus()).isEqualTo(OrderStatus.REJECTED);
        assertThat(result.getRejectionReason()).contains("PRODUCT_NOT_FOUND").contains("PRD-008");
    }

    @Test
    @DisplayName("6.1: rechaza cuando un producto está DISCONTINUED")
    void shouldRejectWhenProductDiscontinued() {
        givenNoPriorActivity();
        Product discontinued = new Product("PRD-001", "Baja", "SKU-1", ProductStatus.DISCONTINUED,
                TaxCategory.STANDARD);
        when(clientsClientPort.getClient(CLIENT_ID)).thenReturn(Optional.of(activeWholesaleClient()));
        when(productsClientPort.getProducts(anySet(), anyString())).thenReturn(Map.of(
                "PRD-001", discontinued,
                "PRD-008", activeStandardProduct("PRD-008")));

        Order result = service.processOrder(specCommand());

        assertThat(result.getStatus()).isEqualTo(OrderStatus.REJECTED);
        assertThat(result.getRejectionReason()).contains("PRODUCT_INACTIVE").contains("PRD-001");
    }

    // ------------------------------------------ matriz de errores externos (7)

    @Test
    @DisplayName("429/5xx/timeout: propaga la excepción transitoria sin persistir ni publicar")
    void shouldPropagateTransientExternalFailure() {
        givenNoPriorActivity();
        when(clientsClientPort.getClient(CLIENT_ID))
                .thenThrow(ExternalServiceException.networkFailure("clients-api", new IOException("timeout")));

        assertThatThrownBy(() -> service.processOrder(specCommand()))
                .isInstanceOf(ExternalServiceException.class)
                .satisfies(ex -> assertThat(((ExternalServiceException) ex).isRetryable()).isTrue());

        verify(orderRepositoryPort, never()).save(any());
        verify(eventPublisherPort, never()).publishOrderProcessed(any());
    }

    @Test
    @DisplayName("Error externo no reintentable (distinto de 404): TECHNICAL_FAILURE persistido y publicado")
    void shouldMarkTechnicalFailureOnDefinitiveExternalError() {
        givenNoPriorActivity();
        when(clientsClientPort.getClient(CLIENT_ID))
                .thenReturn(Optional.of(activeWholesaleClient()));
        when(productsClientPort.getProducts(anySet(), anyString()))
                .thenThrow(ExternalServiceException.forStatus("products-api", 401, "credencial inválida"));

        Order result = service.processOrder(specCommand());

        assertThat(result.getStatus()).isEqualTo(OrderStatus.TECHNICAL_FAILURE);
        assertThat(result.getRejectionReason()).contains("TECHNICAL_FAILURE").contains("products-api");

        ArgumentCaptor<Order> saved = ArgumentCaptor.forClass(Order.class);
        verify(orderRepositoryPort).save(saved.capture());
        verify(eventPublisherPort).publishOrderProcessed(saved.getValue());
    }

    // -------------------------------------------------- contrato de entrada (5.A)

    @Test
    @DisplayName("Violación del contrato → REJECTED persistido y publicado, con el agregado conservado")
    void shouldRejectOnContractViolationWhenAggregateBuildable() {
        givenNoPriorActivity();
        var command = new ProcessOrderCommand(EVENT_ID, 1L, ORDER_ID, "MX", "MXN", CLIENT_ID, "C1",
                List.of(new OrderLineCommand("PRD-001", 24, new BigDecimal("35.5")),
                        new OrderLineCommand("PRD-001", 12, new BigDecimal("82.0"))));

        Order result = service.processOrder(command);

        assertThat(result.getStatus()).isEqualTo(OrderStatus.REJECTED);
        assertThat(result.getRejectionReason())
                .contains("CONTRACT_VIOLATION").contains("items productId duplicado");
        assertThat(result.getOrderId()).isEqualTo(ORDER_ID);
        assertThat(result.getItems()).hasSize(2);
        verify(clientsClientPort, never()).getClient(anyString());
        verify(eventPublisherPort).publishOrderProcessed(result);
    }

    @Test
    @DisplayName("Violación que impide construir el agregado → REJECTED con orderId sintético")
    void shouldRejectOnContractViolationWhenAggregateNotBuildable() {
        givenNoPriorActivity();
        var command = new ProcessOrderCommand(EVENT_ID, 1L, null, "MX", "MXN", CLIENT_ID, "C1",
                List.of(new OrderLineCommand("PRD-001", 24, new BigDecimal("35.5"))));

        Order result = service.processOrder(command);

        assertThat(result.getStatus()).isEqualTo(OrderStatus.REJECTED);
        assertThat(result.getRejectionReason()).contains("orderId es obligatorio");
        assertThat(result.getOrderId()).isEqualTo("UNPARSEABLE-" + EVENT_ID);
        assertThat(result.getItems()).hasSize(1);
    }

    // ------------------------------------------- mensaje no parseable / reintentos

    @Test
    @DisplayName("Mensaje no parseable → REJECTED persistido y publicado conservando el payload crudo")
    void shouldRecordUnprocessableMessage() {
        when(orderRepositoryPort.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        Order result = service.recordUnprocessable(new ProcessOrderUseCase.UnprocessableCommand(
                ORDER_ID, EVENT_ID, "MX", "MXN", null, "DESERIALIZATION: JSON inválido",
                "{\"rotto\":true}"));

        assertThat(result.getStatus()).isEqualTo(OrderStatus.REJECTED);
        assertThat(result.getOrderId()).isEqualTo(ORDER_ID);
        assertThat(result.getEventVersion()).isZero();
        assertThat(result.getClientId()).isNull();
        assertThat(result.getMarket()).isNotNull();
        assertThat(result.getRejectionReason()).contains("DESERIALIZATION");
        assertThat(result.getSourcePayload()).isEqualTo("{\"rotto\":true}");
        verify(clientsClientPort, never()).getClient(anyString());
        verify(eventPublisherPort).publishOrderProcessed(result);
    }

    @Test
    @DisplayName("Reintentos agotados → TECHNICAL_FAILURE persistido y publicado sin consultar contexto")
    void shouldRecordTechnicalFailureWhenRetriesExhausted() {
        givenNoPriorActivity();

        Order result = service.recordTechnicalFailure(specCommand(), "backoff agotado");

        assertThat(result.getStatus()).isEqualTo(OrderStatus.TECHNICAL_FAILURE);
        assertThat(result.getRejectionReason()).isEqualTo("TECHNICAL_FAILURE: backoff agotado");
        assertThat(result.getItems()).hasSize(2);
        verify(clientsClientPort, never()).getClient(anyString());
        verify(productsClientPort, never()).getProducts(anySet(), anyString());
        verify(eventPublisherPort).publishOrderProcessed(result);
    }
}
