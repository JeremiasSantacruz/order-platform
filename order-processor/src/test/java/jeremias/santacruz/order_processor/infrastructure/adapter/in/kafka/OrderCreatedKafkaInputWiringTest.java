package jeremias.santacruz.order_processor.infrastructure.adapter.in.kafka;

import jeremias.santacruz.order_processor.application.OrderProcessingService;
import jeremias.santacruz.order_processor.domain.port.in.ProcessOrderUseCase;
import jeremias.santacruz.order_processor.domain.port.out.ClientsClientPort;
import jeremias.santacruz.order_processor.domain.port.out.EventPublisherPort;
import jeremias.santacruz.order_processor.domain.port.out.OrderRepositoryPort;
import jeremias.santacruz.order_processor.domain.port.out.ProductsClientPort;
import jeremias.santacruz.order_processor.infrastructure.adapter.out.http.ClientsClientAdapter;
import jeremias.santacruz.order_processor.infrastructure.adapter.out.http.ProductsClientAdapter;
import jeremias.santacruz.order_processor.infrastructure.adapter.out.kafka.EventPublisherAdapter;
import jeremias.santacruz.order_processor.infrastructure.adapter.out.mongo.OrderRepositoryAdapter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cableado de la interfaz de entrada con el contexto completo de Spring: el listener, su
 * contenedor (ack-mode, cabecera de intento, tópico) y la resolución de todos los puertos del
 * dominio hacia sus adaptadores.
 */
@SpringBootTest
class OrderCreatedKafkaInputWiringTest {

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Autowired
    private OrderCreatedListener listener;

    @Autowired
    private OrderCreatedEventValidator validator;

    @Autowired
    private OrderEventJsonReader reader;

    @Autowired
    private DltPublishingRecoverer recoverer;

    @Autowired
    private DefaultErrorHandler errorHandler;

    @Autowired
    private ProcessOrderUseCase useCase;

    @Autowired
    private EventPublisherPort eventPublisherPort;

    @Autowired
    private ClientsClientPort clientsClientPort;

    @Autowired
    private ProductsClientPort productsClientPort;

    @Autowired
    private OrderRepositoryPort orderRepositoryPort;

    @Test
    @DisplayName("El listener corre en un contenedor con ack-mode record y cabecera de intento")
    void listenerContainerIsConfiguredForDltMetadata() {
        MessageListenerContainer container = registry.getListenerContainer("ordersCreatedListener");

        assertThat(container).isNotNull();
        assertThat(listener).isNotNull();
        assertThat(validator).isNotNull();
        assertThat(reader).isNotNull();

        ConcurrentMessageListenerContainer<?, ?> kafkaContainer =
                (ConcurrentMessageListenerContainer<?, ?>) container;
        ContainerProperties properties = kafkaContainer.getContainerProperties();
        assertThat(properties.getAckMode()).isEqualTo(ContainerProperties.AckMode.RECORD);
        assertThat(properties.isDeliveryAttemptHeader()).isTrue();
        assertThat(properties.getTopics()).contains("orders.created.v1");
    }

    @Test
    @DisplayName("El manejador de errores de la sección 7 está publicado como bean único")
    void errorHandlerBeanIsWired() {
        assertThat(errorHandler).isNotNull();
        assertThat(recoverer).isNotNull();
    }

    @Test
    @DisplayName("Todos los puertos de salida resuelven a sus adaptadores de infraestructura")
    void outboundPortsResolveToAdapters() {
        assertThat(useCase).isInstanceOf(OrderProcessingService.class);
        assertThat(eventPublisherPort).isInstanceOf(EventPublisherAdapter.class);
        assertThat(clientsClientPort).isInstanceOf(ClientsClientAdapter.class);
        assertThat(productsClientPort).isInstanceOf(ProductsClientAdapter.class);
        assertThat(orderRepositoryPort).isInstanceOf(OrderRepositoryAdapter.class);
    }
}
