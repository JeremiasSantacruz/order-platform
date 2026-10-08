package jeremias.santacruz.order_processor.infrastructure.config;

import jeremias.santacruz.order_processor.infrastructure.adapter.in.kafka.ProcessingFailureRecoverer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

/**
 * Configuración de la interfaz de entrada Kafka.
 *
 * <p>Spring Boot detecta el {@link DefaultErrorHandler} como {@code CommonErrorHandler} y lo
 * aplica al contenedor de los {@code @KafkaListener}, por lo que no hace falta recrear la fábrica
 * de contenedores.</p>
 *
 * <p>Política de errores:</p>
 * <ul>
 *   <li>Payload no parseable y violaciones de contrato: se resuelven dentro del listener/caso de
 *       uso como pedidos {@code REJECTED}, sin pasar por este manejador.</li>
 *   <li>Resto de fallos (429, 5xx, timeout, fallos internos): reintento con backoff exponencial;
 *       al agotarlos, {@link ProcessingFailureRecoverer} registra el estado
 *       {@code TECHNICAL_FAILURE}, que se persiste y se publica en {@code orders.processed.v1}.</li>
 * </ul>
 */
@Configuration
public class KafkaConsumerConfig {

    /**
     * Política de errores del consumidor: reintenta con backoff exponencial y, cuando no hay más
     * intentos, registra el resultado técnico definitivo del mensaje.
     *
     * @param recoverer       Registra el pedido tras agotar los reintentos
     * @param maxReintentos   Número de reintentos para fallos transitorios
     */
    @Bean
    DefaultErrorHandler ordersCreatedErrorHandler(ProcessingFailureRecoverer recoverer,
                                                  @Value("${app.kafka.consumer.max-retries:3}") int maxReintentos) {
        if (maxReintentos < 0) {
            throw new IllegalArgumentException("app.kafka.consumer.max-retries no puede ser negativo");
        }
        // Backoff: intervalo inicial 1s y multiplicador 2 (valores por defecto de Spring Kafka).
        return new DefaultErrorHandler(recoverer, new ExponentialBackOffWithMaxRetries(maxReintentos));
    }
}