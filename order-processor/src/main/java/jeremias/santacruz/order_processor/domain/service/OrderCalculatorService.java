package jeremias.santacruz.order_processor.domain.service;

import jeremias.santacruz.order_processor.domain.model.client.Client;
import jeremias.santacruz.order_processor.domain.model.order.EnrichedOrderLine;
import jeremias.santacruz.order_processor.domain.model.order.Market;
import jeremias.santacruz.order_processor.domain.model.order.OrderLine;
import jeremias.santacruz.order_processor.domain.model.order.OrderTotals;
import jeremias.santacruz.order_processor.domain.model.product.Product;
import jeremias.santacruz.order_processor.domain.model.product.TaxCategory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;

public class OrderCalculatorService {

    private static final BigDecimal WHOLESALE_DISCOUNT_RATE = new BigDecimal("0.03");
    private static final int WHOLESALE_MIN_QUANTITY = 20;

    /**
     * Si crece esta logica del calculo lo mejor seria dividirla en distintas estrategias usando interfaces
     * Si crece el volumen de paises pero la logica no cambia es mejor una carga a traves de una Base de datos
     * Microservicio fuente
     */
    private static final Map<Market, Map<TaxCategory, BigDecimal>> TAX_RATES = Map.of(
            Market.MX, Map.of(
                    TaxCategory.STANDARD, new BigDecimal("0.16"),
                    TaxCategory.REDUCED, new BigDecimal("0.08"),
                    TaxCategory.EXEMPT, BigDecimal.ZERO
            ),
            Market.CO, Map.of(
                    TaxCategory.STANDARD, new BigDecimal("0.19"),
                    TaxCategory.REDUCED, new BigDecimal("0.05"),
                    TaxCategory.EXEMPT, BigDecimal.ZERO
            ),
            Market.PE, Map.of(
                    TaxCategory.STANDARD, new BigDecimal("0.18"),
                    TaxCategory.REDUCED, new BigDecimal("0.10"),
                    TaxCategory.EXEMPT, BigDecimal.ZERO
            )
    );

    public EnrichedOrderLine calculateLine(OrderLine item, Client client, Product product, Market market) {
        BigDecimal grossSubtotal = item.unitPrice()
                .multiply(BigDecimal.valueOf(item.quantity()))
                .setScale(2, RoundingMode.HALF_UP);

        BigDecimal discountRate = BigDecimal.ZERO;
        if (client.isWholesale() && item.quantity() >= WHOLESALE_MIN_QUANTITY) {
            discountRate = WHOLESALE_DISCOUNT_RATE;
        }
        BigDecimal discount = grossSubtotal.multiply(discountRate).setScale(2, RoundingMode.HALF_UP);
        BigDecimal netSubtotal = grossSubtotal.subtract(discount).setScale(2, RoundingMode.HALF_UP);

        BigDecimal taxRate = getEffectiveTaxRate(market, product.taxCategory(), client);
        BigDecimal taxAmount = netSubtotal.multiply(taxRate).setScale(2, RoundingMode.HALF_UP);
        BigDecimal lineTotal = netSubtotal.add(taxAmount).setScale(2, RoundingMode.HALF_UP);

        return new EnrichedOrderLine(item.productId(), item.quantity(), item.unitPrice(),
                product.name(), product.sku(), product.taxCategory(), taxRate, discountRate,
                grossSubtotal, discount, netSubtotal, taxAmount, lineTotal);
    }

    public OrderTotals calculateTotals(List<EnrichedOrderLine> lines) {
        BigDecimal grossSubtotal = BigDecimal.ZERO;
        BigDecimal discount = BigDecimal.ZERO;
        BigDecimal netSubtotal = BigDecimal.ZERO;
        BigDecimal tax = BigDecimal.ZERO;
        BigDecimal grandTotal = BigDecimal.ZERO;

        for (EnrichedOrderLine line : lines) {
            grossSubtotal = grossSubtotal.add(line.grossSubtotal());
            discount = discount.add(line.discount());
            netSubtotal = netSubtotal.add(line.netSubtotal());
            tax = tax.add(line.taxAmount());
            grandTotal = grandTotal.add(line.lineTotal());
        }

        return new OrderTotals(grossSubtotal, discount, netSubtotal, tax, grandTotal);
    }

    private BigDecimal getEffectiveTaxRate(Market market, TaxCategory category, Client client) {
        if (client.isExempt()) {
            return BigDecimal.ZERO;
        }
        return TAX_RATES.getOrDefault(market, Map.of())
                .getOrDefault(category, BigDecimal.ZERO);
    }
}