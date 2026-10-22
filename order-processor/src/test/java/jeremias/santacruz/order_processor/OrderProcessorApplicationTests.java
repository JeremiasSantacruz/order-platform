package jeremias.santacruz.order_processor;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class OrderProcessorApplicationTests {

	@Autowired
	private ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory;

	@Test
	void contextLoads() {
	}

	@Test
	void kafkaListenerRunsOnVirtualThreads() throws Exception {
		AsyncTaskExecutor listenerExecutor = kafkaListenerContainerFactory.getContainerProperties()
				.getListenerTaskExecutor();

		assertThat(listenerExecutor).isNotNull();
		Thread listenerThread = listenerExecutor.submit(Thread::currentThread).get(5, TimeUnit.SECONDS);
		assertThat(listenerThread.isVirtual())
				.as("spring.threads.virtual.enabled debe configurar el listener de Kafka con hilos virtuales")
				.isTrue();
	}

}
