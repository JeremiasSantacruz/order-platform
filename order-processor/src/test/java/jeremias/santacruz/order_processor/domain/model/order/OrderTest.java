package jeremias.santacruz.order_processor.domain.model.order;


import jeremias.santacruz.order_processor.domain.model.client.Client;
import jeremias.santacruz.order_processor.domain.model.client.ClientSegment;
import jeremias.santacruz.order_processor.domain.model.client.ClientStatus;
import jeremias.santacruz.order_processor.domain.model.client.TaxRegime;

import jeremias.santacruz.order_processor.domain.model.product.TaxCategory;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderTest {


    @Test
    @DisplayName("Debe fallar al crear una orden sin items")
    void shouldFailWhenItemsListIsEmpty() {
        assertThatThrownBy(() -> new Order(
                "ORD-1", "EVT-1", 1L, Market.MX, Currency.MXN, "CLI-1", Collections.emptyList(), Instant.now()
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one item");
    }

    @Test
    @DisplayName("Debe fallar si la moneda no corresponde al mercado enviado")
    void shouldFailWhenCurrencyDoesNotMatchMarket() {
        List<OrderLine> items = List.of(new OrderLine("PRD-1", 5, new BigDecimal("10.00")));

        assertThatThrownBy(() -> new Order(
                "ORD-1", "EVT-1", 1L, Market.MX, Currency.COP, "CLI-1", items, Instant.now()
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not valid for market");
    }

    @Test
    @DisplayName("Debe aprobar la orden asignando los totales correctamente")
    void shouldApproveOrderSuccessfully() {
        List<OrderLine> items = List.of(new OrderLine("PRD-1", 5, new BigDecimal("10.00")));
        Order order = new Order("ORD-1", "EVT-1", 1L, Market.MX, Currency.MXN, "CLI-1", items, Instant.now());

        OrderTotals totals = new OrderTotals(
                new BigDecimal("50.00"), BigDecimal.ZERO, new BigDecimal("50.00"), new BigDecimal("8.00"), new BigDecimal("58.00")
        );

order.approve(totals, List.of(), null, Instant.now());

        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
        assertThat(order.getTotals()).isEqualTo(totals);
        assertThat(order.getRejectionReason()).isNull();
    }

    @Test
    @DisplayName("Debe conservar el snapshot del cliente y las líneas enriquecidas al aprobar")
    void shouldStoreClientSnapshotAndEnrichedLinesOnApprove() {
        List<OrderLine> items = List.of(new OrderLine("PRD-1", 5, new BigDecimal("10.00")));
        Order order = new Order("ORD-1", "EVT-1", 1L, Market.MX, Currency.MXN, "CLI-1", items, Instant.now());

        Client client = new Client("CLI-1", "Cliente A", ClientStatus.ACTIVE, ClientSegment.WHOLESALE,
                TaxRegime.GENERAL, Market.MX);
        EnrichedOrderLine line = new EnrichedOrderLine("PRD-1", 5, new BigDecimal("10.00"), "Refresco",
                "SKU-1", TaxCategory.STANDARD, new BigDecimal("0.16"), BigDecimal.ZERO,
                new BigDecimal("50.00"), BigDecimal.ZERO, new BigDecimal("50.00"),
                new BigDecimal("8.00"), new BigDecimal("58.00"));
        OrderTotals totals = new OrderTotals(new BigDecimal("50.00"), BigDecimal.ZERO,
                new BigDecimal("50.00"), new BigDecimal("8.00"), new BigDecimal("58.00"));

        order.approve(totals, List.of(line), client, Instant.now());

        assertThat(order.getClient()).isEqualTo(client);
        assertThat(order.getEnrichedLines()).containsExactly(line);
    }

    @Test
    @DisplayName("La fábrica partial tolera campos ausentes y soporta estado final")
    void shouldBuildPartialWithoutRequiredFieldsThenReject() {
        Order order = Order.partial("UNPARSEABLE-x", "EVT-9", 0L, null, null, null, List.of(), Instant.now());

        assertThat(order.getOrderId()).isEqualTo("UNPARSEABLE-x");
        assertThat(order.getMarket()).isNull();
        assertThat(order.getItems()).isEmpty();

        order.reject("CONTRACT_VIOLATION: orderId es obligatorio", Instant.now());

        assertThat(order.getStatus()).isEqualTo(OrderStatus.REJECTED);
        assertThat(order.getTotals().grandTotal()).isEqualByComparingTo("0");
    }
}