package jeremias.santacruz.order_processor.domain.model.order;


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

        order.approve(totals, Instant.now());

        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
        assertThat(order.getTotals()).isEqualTo(totals);
        assertThat(order.getRejectionReason()).isNull();
    }

}