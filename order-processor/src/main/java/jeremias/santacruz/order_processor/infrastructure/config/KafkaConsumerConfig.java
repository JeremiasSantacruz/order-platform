package jeremias.santacruz.order_processor.infrastructure.config;

import jeremias.santacruz.order_processor.infrastructure.adapter.in.kafka.ContractViolationException;
import jeremias.santacruz.order_processor.infrastructure.adapter.in.kafka.DltPublishingRecoverer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

/**
 * Configuración de la interfaz de entrada Kafka.
 *
 * <p>Spring Boot detecta el {@link DefaultErrorHandler} como {@code CommonErrorHandler} y lo
 * aplica al contenedor de los {@code @KafkaListener}, por lo que no hace falta recrear la fábrica
 * de contenedores.</p>
 *
 * <p>Política de errores (sección 7 de Especificaciones.md):</p>
 * <ul>
 *   <li>Violaciones de contrato (payload inválido o reglas de entrada incumplidas): fallo
 *       definitivo, sin reintentos, directo a la DLT.</li>
 *   <li>Resto de fallos (429, 5xx, timeout, fallos internos): reintento con backoff exponencial;
 *       al agotarlos, a la DLT.</li>
 * </ul>
 */
@Configuration
public class KafkaConsumerConfig {

    /**
     * Política de errores del consumidor: reintenta con backoff exponencial y publica en la DLT
     * cuando no hay más intentos disponibles.
     *
     * @param recoverer Publicador de mensajes fallidos en la DLT
     * @param maxReintentos Número de reintentos para fallos transitorios
     */
    @Bean
    DefaultErrorHandler ordersCreatedErrorHandler(DltPublishingRecoverer recoverer,
                                                  @Value("${app.kafka.consumer.max-retries:3}") int maxReintentos) {
        if (maxReintentos < 0) {
            throw new IllegalArgumentException("app.kafka.consumer.max-retries no puede ser negativo");
        }
        // Backoff: intervalo inicial 1s y multiplicador 2 (valores por defecto de Spring Kafka).
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer,
                new ExponentialBackOffWithMaxRetries(maxReintentos));
        errorHandler.addNotRetryableExceptions(ContractViolationException.class);
        return errorHandler;
    }

    /**
     * Habilita la cabecera {@code KafkaHeaders.DELIVERY_ATTEMPT} en cada registro, para que la
     * DLT declare el número real de intentos en {@code attemptCount}.
     */
    @Bean
    ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>>
            deliveryAttemptHeaderCustomizer() {
        return container -> container.getContainerProperties().setDeliveryAttemptHeader(true);
    }
}
