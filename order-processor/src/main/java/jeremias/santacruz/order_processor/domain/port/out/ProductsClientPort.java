package jeremias.santacruz.order_processor.domain.port.out;

import jeremias.santacruz.order_processor.domain.model.product.Product;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface ProductsClientPort {
    /**
     * Consulta los detalles de múltiples productos para un mercado específico.
     *
     * @param productIds Conjunto de IDs de productos a consultar
     * @param market Mercado correspondiente a la búsqueda
     * @return Mapa asociando productId con su modelo de dominio Product
     */
    Map<String, Product> getProducts(Set<String> productIds, String market);

    /**
     * Consulta individual de un producto por su ID y mercado.
     */
    Optional<Product> getProduct(String productId, String market);
}
