# Order-Processor Wiki

**Proyecto:** Order Processor — Microservicio de procesamiento de pedidos  
**Tecnología:** Spring Boot 3.5 + Gradle  
**Arquitectura:** Limpia (Clean Architecture)  
**Lenguaje:** Java 21  

---

## 📋 Tabla de Contenidos

1. [Visión General](#visión-general)
2. [Arquitectura Limpia](#arquitectura-limpia)
3. [Componentes Principales](#componentes-principales)
4. [Flujo de Procesamiento de Pedidos](#flujo-de-procesamiento-de-pedidos)
5. [Modelos de Dominio](#modelos-de-dominio)
6. [Servicios del Core](#servicios-del-core)
7. [Puertos y Adaptadores](#puertos-y-adaptadores)
8. [Resiliencia y Throttling](#resiliencia-y-throttling)
9. [Persistencia](#persistencia)
10. [Ingesta de Eventos](#ingesta-de-eventos)
11. [Ejecución y Deployment](#ejecución-y-deployment)
12. [Configuración](#configuración)

---

## Visión General

Este microservicio consume eventos de pedidos (`orders.created.v1`) desde Kafka, procesa pedidos enriqueciendo información del cliente y productos, calculando totales con impuestos, y publica eventos de estado final (`orders.processed.v1`).

**Características clave:**
- **Idempotencia absoluta**: re-entrega del mismo evento no duplica efectos.
- **Control de versión por orderId**: eventos obsoletos se ignoran; concurrentes se resuelven con optimismo.
- **Validación contratada**: violaciones no fallan, generan estados `REJECTED`.
- **Resiliencia**: manejo explícito de fallos transitorios (backoff) y definitivos.
- **Logs JSON estructurados**: eventId/orderId en cada log para Grafana/Loki.

---

## Arquitectura Limpia

La aplicación sigue los principios de Clean Architecture:

```
┌─────────────────────────────────────────────────┐
│                    Use Cases                     │
│  (Application / Business Logic)                 │
└─────────────────────────────────────────────────┘
           ▼ ◄► ▲
┌──────────────────┐  ┌───────────────────────────────┐
│     Domain Ports  │◄─►  External Ports               │
│    (interfaces)   │  Kafka | HTTP Clients            │
└──────────────────┘  └────────────────────────────────┘

          ▼ ▲
┌─────────────────────────────────────────────────┐
│ Interchange Layer - Infrastructure Adaptadores │
│ Kafka | MongoDB | HTTP (clients-api, products)   │
└─────────────────────────────────────────────────┘
```

**Capa de Dominio:** puertos independientes de tecnologías externas.  
**Capa de Aplicación:** orquesta los casos de uso y adapta puertos externos.  
**Capa de Infraestructura:** implementaciones concretas (Kafka, MongoDB, HTTP).

---

## Componentes Principales

### 1. **Main Application** (`OrderProcessorApplication`)
Punto de entrada Spring Boot; bootstrap del contenedor principal.

### 2. **Use Case** (`ProcessOrderUseCase`)
- `processOrder(ProcessOrderCommand)` → procesa evento exitoso.
- `recordUnprocessable(UnprocessableCommand)` → mensaje sin campo eventId/orderId válido.
- `recordTechnicalFailure()` → error tras reintentos agotados.

### 3. **Servicios de Dominio**  
- `OrderContractService`: validación del contrato de entrada.  
- `OrderEligibilityService`: reglas de elegibilidad (cliente activo, productos activos, mercado coincidente).  
- `OrderCalculatorService`: cálculo financiero con impuestos y descuentos por volumen.

### 4. **Adaptadores de Entrada**:
- `OrderCreatedListener`: consume Kafka, deserializa, mapea a comando del uso de caso.  
- `ProcessingFailureRecoverer`: recovery para eventos fallidos almacenados en DB.

### 5. **Adaptadores de Salida**:
- `ClientsClientAdapter`, `ProductsClientAdapter`: clientes HTTP hacia microservicios externos sin web server implícito.  
- `OrderRepositoryPort` → `MongoDB`.  
- `EventPublisherPort` → Kafka `orders.processed.v1`.

---

## Flujo de Procesamiento de Pedidos

```
Kafka (orders.created.v1)
    │
    ▼
┌─────────────────────┐
│ OrderCreatedListener │  Deserializa, extrae eventId/orderId/clientId/items.
└──────────┬──────────┘
           │
           ▼
   ┌─────────────────────┐
   │ validateContract()   │ Si falla → guarda estado REJECTED y persiste.
   └──────────┬──────────┘
              │
              ├─► Client exists? NO → state = REJECTED (CLIENT_NOT_FOUND).
              │
              ├─► Productos activos? NO → rejected reason por producto, persiste.
              │
              ├─► Cliente activo y productos ok: sí.
              │
              ▼
    ┌───────────────────────┐
    │ Calculo Financiero      │ EnrichLines (impuestos + descuentos).
    │ Client/Products lookup  │ Si transitorio → reintentan con backoff.
    │                         │ Si definitivo → estado = TECHNICAL_FAILURE.
    └──────────┬─────────────┘
               │
               ▼
    ┌───────────────────┐
    │ persist + publish │ Al MongoDB y Kafka (orders.processed.v1).
    └───────────────────┘
              │
              ▼
         Done!
```

**Idempotencia:** re-entrega del mismo `eventId` devuelve el pedido ya resuelto (resolveExisting).  
**Control de versiones:** evento con versión menor que la persistida se ignora.

---

## Modelos de Dominio

### 1. **Order**
El agregado principal que encapsula todo lo necesario para representar un pedido. Campos inmutables:

- `orderId`: identificador único del negocio.  
- `eventId`: identificador del evento de entrada (Kafka).  
- `eventVersion`: versión para control optimista de concurrencia.  
- `market`: mercado (`MX`, `CO`, `PE`).  
- `currency`: moneda soportada por el mercado.  
- `clientId`: cliente del pedido.  
- `items`: lista implícita de `OrderLine`.  
- Mutables (estado final):
  - `status` (`APPROVED | REJECTED | TECHNICAL_FAILURE`).
  - `rejectionReason/string` (cuando fue rechazado).
  - `processedAt/Totals/client/enrichedLines` (solo si aprobado).

Estados finales: todos los mensajes que llegan al sistema quedan guardados en el DB. No existe un destino paralelo para fallos (salvamos snapshot).

### 2. **OrderLine**
Línea de pedido con `productId`, `quantity`, `unitPrice`.  
Luego se enriquece en `EnrichedOrderLine` con impuestos, descuentos, y totales por línea.

### 3. **Client**
Registro con:
- `clientId`, `name`.  
- `status`: ACTIVE/INACTIVE/DISCONTINUED (solo ACTIVE es válido).  
- `segment`: WHOLESALE | RETAIL (afecta descuentos).  
- `taxRegime`: EXEMPT | STAN. (regímenes fiscales del cliente).  
- `market`: región del cliente (`MX | CO | PE`).

### 4. **Product**
Producto con:
- `productId`, `name`, `sku`.  
- `status` (ACTIVE/DISCONTINUED).  
- `taxCategory`: STANDARD/REDUCED/EXEMPT (afecta tasa impositiva).

---

## Servicios del Core

### 1. **OrderCalculatorService**
Cálculo con reglas de negocio puros:

- Descuento por volumen si cliente es mayorista (`WHOLESALE`) y quantity ≥ 20 unidades: 3%.  
- Tasa fiscal por `market` + `taxCategory` (ver matriz TAX_RATES).  
- Si cliente está exento, tasa = 0 sin importar el régimen.

Todos los cálculos usan `BigDecimal` y `RoundingMode.HALF_UP` para evitar errores de punto flote.

### 2. **OrderEligibilityService**
Valida elegibilidad:
1. Client active?  
2. Market coincide?  
3. Productos activos?  

Devuelve `Optional<String>` con razón del rechazo. Si hay varios fallos, el primero (prioridad: client inactivo > producto no encontrado > producto inactivo) marca el estado `REJECTED`.

### 3. **OrderContractService**
Valida que la entrada cumpla las reglas contractuales antes de intentar procesar. Violaciones típicas:
- Mercado inválido o moneda no soportada por ese mercado.  
- Items vacíos o nulls.  
- Campos obligatorios ausentes (orderId/eventId/clientId).  

Devuelve `List<String>` de razones de contrato violado; convertimos a estado `REJECTED`.

---

## Puertos y Adaptadores

### Puertos de Salida

| Puerto                              | Implementación                                              | Descripción                           |
|-------------------------------------|-------------------------------------------------------------|---------------------------------------|
| ClientsClientPort                   | ClientsClientAdapter (HTTP)                                 | Llama /clients/{id} a clients-api    |
| ProductsClientPort                  | ProductsClientAdapter (HTTP)                                | Llama /products?market={m}           |
| OrderRepositoryPort                 | OrderRepositoryAdapter (Mongo)                              | CRUD en MongoDB collection          |
| EventPublisherPort                  | KafkaProducer implícito por AutoConfiguration               | Publica a orders.processed.v1       |

### Puertos de Entrada

- `ProcessOrderUseCase` → Implementado por `OrderProcessingService`.  
- `UnprocessableCommand` → usado para mensajes corruptos que no pudieron parsearse.

---

## Resiliencia y Throttling

El sistema usa:
- `@RateLimiter` explícito (no el interceptor global del servidor web; este microservicio no corre HTTP server, solo HTTP client).  
- Contenedor de reintentos: manejador de errores de Kafka lanza backoff exponencial para respuestas 429/5xx/tiempos de espera.  
- `RetryableExternalServicePredicate`: decide si es retryable por tipo de error (HTTP status code o timeout).  

Matriz de errores del cliente externo:
- **Transitorio** → reintentar con backoff exponencial (429, 5xx, timeouts > 30s).  
- **Definitivo** → guardar estado `TECHNICAL_FAILURE` (4xx permanentes o desconexión definitiva tras agotar reintentos del listener de Kafka).

---

## Persistencia

#### MongoDB
Repositorio para el agregado `Order`. El mapeo Java -> Aggregado Mongo:
- Campos inmutables (`orderId`, `eventId`, `status` etc.) se persisten directamente.  
- `sourcePayload`: snapshot del JSON original del evento (solo rellenó cuando el mensaje no pudo procesarse). Campo clave para salvar incidentes inexplicables por metadata.

Si la carrera de concurrencia entre versiones ocurre, el repositorio resuelve con atomicidad; si el estado persistido pertenece a otro evento, no se publica esa salida (la publicación corre solo si coincide `eventId`).

---

## Ingesta de Eventos

#### Kafka Consumer
- Topic: `$app.kafka.topics.orders-created` (default: `orders.created.v1`).  
- Listener ID: `ordersCreatedListener`.  
- Serialización JSON con Jackson (deserializa a DTO de contrato).  
- MDC (Mapped Diagnostic Context): cada log lleva campos raíz `eventId`, `orderId` para visualización en Grafana/Loki.  

Manejo de fallos de deserialización: se registra como pedido no procesable, se extrae lo que sí pudo leerse (salvage) y se persiste con estado `REJECTED`.

---

## Ejecución y Deployment

### Build
```bash
cd order-processor
./gradlew bootJar
```

### Dockerfile
Multi-stage build:
1. Gradle builder JVM 21 → compila JAR sin ejecutar pruebas.  
2. Eclipse Temurin 21 (Alpine) → ejecuta el JAR empaquetado.  

Port expuesto: 8080 (Spring Boot por defecto, aunque no expone API REST).

### Ejecución local
```bash
./gradlew bootRun --args='--spring.config.additional-location=classpath:/app'
```
Configuración de Kafka/MongoDB desde `application.yml` o propiedades del entorno.

### Variables de entorno recomendadas:
```bash
APP_KAFKA_TOPICS_ORDERS_CREATED=orders.created.v1
SPRING_DATASOURCE_URL=jdbc:mysql://mongo:27017/orders?databaseName=orders
MONGO_HOST=mongo:27017
```

---

## Configuración

### Kafka bootstrap: `spring.kafka.bootstrap-servers`  
### MongoDB URL: `spring.data.mongodb.url` o `SPRING_DATASOURCE_URL` según setup.  
### Throttling: usar propiedades de Resilience4J (`resilience4j.ratelimiter.config.<service>.maxSignals`).

---

## 📚 Referencia Adicional

- Readme del repositorio padre (`order-platform/README.md`) para despliegue y topologías.  
- Especificación `Expecificaciones.md` para detalles de integración con Clients API / Products API.  

---

*Generado automáticamente el 15 de octubre de 2026 | Order-Processor v0.0.1-SNAPSHOT*
