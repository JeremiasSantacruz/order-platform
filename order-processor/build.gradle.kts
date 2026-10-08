plugins {
	java
	id("org.springframework.boot") version "3.5.16"
	id("io.spring.dependency-management") version "1.1.7"
}

group = "jeremias.santacruz"
version = "0.0.1-SNAPSHOT"

java {
	toolchain {
		languageVersion = JavaLanguageVersion.of(21)
	}
}

repositories {
	mavenCentral()
}

dependencies {
	implementation("org.springframework.boot:spring-boot-starter")
	// Interfaz de entrada: consumidor de Kafka (orders.created.v1)
	implementation("org.springframework.kafka:spring-kafka")
	// Deserialización del contrato JSON de entrada
	implementation("com.fasterxml.jackson.core:jackson-databind")
	// Interfaz de salida: cliente HTTP (Clients API / Products API) sin servidor web
	implementation("org.springframework:spring-web")
	// Throttling saliente (@RateLimiter + AOP) hacia Clients/Products API; ver resilience4j.*
	implementation("io.github.resilience4j:resilience4j-spring-boot3:2.3.0")
	implementation("org.springframework.boot:spring-boot-starter-aop")
	// Interfaz de salida: persistencia del agregado Order en MongoDB
	implementation("org.springframework.boot:spring-boot-starter-data-mongodb")
	// Logs estructurados en JSON (stdout) para Loki/Grafana: eventId y orderId
	// quedan como campos raíz del evento gracias al MDC.
	implementation("net.logstash.logback:logstash-logback-encoder:8.1")

	testImplementation("org.springframework.boot:spring-boot-starter-test")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
	// Pruebas de integración de la persistencia Mongo (se omiten si no hay Docker)
	testImplementation("org.testcontainers:junit-jupiter")
	testImplementation("org.testcontainers:mongodb")
	// Broker Kafka embebido para pruebas de flujo produce → procesa → consume
	testImplementation("org.springframework.kafka:spring-kafka-test")
}

tasks.withType<JavaCompile> {
	options.encoding = "UTF-8"
}

tasks.withType<Test> {
	useJUnitPlatform()
}
