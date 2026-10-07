package jeremias.santacruz.order_processor.domain.model.order;

import java.math.BigDecimal;

public record OrderTotals(
        BigDecimal grossSubtotal,
        BigDecimal discount,
        BigDecimal netSubtotal,
        BigDecimal taxAmount,
        BigDecimal total
) {
    public static OrderTotals zero() {
        return new OrderTotals(BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO);
    }

    public BigDecimal tax(){
        return taxAmount;
    }

    public BigDecimal grandTotal(){
        return total;
    }
}
