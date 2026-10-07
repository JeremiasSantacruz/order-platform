package jeremias.santacruz.order_processor.domain.model.order;

import java.math.BigDecimal;

public record OrderLine(
        String productId,
        Integer quantity,
        BigDecimal unitPrice
) {
}
