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
	// Contrato de entrada: validación de payload (jakarta validation)
	implementation("org.springframework.boot:spring-boot-starter-validation")
	// Interfaz de entrada: consumidor de Kafka (orders.created.v1 / orders.processing.dlt)
	implementation("org.springframework.kafka:spring-kafka")
	// Deserialización del contrato JSON de entrada
	implementation("com.fasterxml.jackson.core:jackson-databind")
	// Interfaz de salida: cliente HTTP (Clients API / Products API) sin servidor web
	implementation("org.springframework:spring-web")
	// Interfaz de salida: persistencia del agregado Order en MongoDB
	implementation("org.springframework.boot:spring-boot-starter-data-mongodb")

	testImplementation("org.springframework.boot:spring-boot-starter-test")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
	// Pruebas de integración de la persistencia Mongo (se omiten si no hay Docker)
	testImplementation("org.testcontainers:junit-jupiter")
	testImplementation("org.testcontainers:mongodb")
}

tasks.withType<JavaCompile> {
	options.encoding = "UTF-8"
}

tasks.withType<Test> {
	useJUnitPlatform()
}
