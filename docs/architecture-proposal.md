# Propuesta de Arquitectura — Plataforma Confiable de Pedidos B2B

## 1. Contexto y Objetivos

- **Dominio:** procesamiento de pedidos B2B originados como eventos Kafka (`orders.created.v1`).
- **Responsabilidad del worker:** validar el evento, verificar elegibilidad contra los servicios de
  catálogo, calcular importes con precisión financiera y emitir el evento `orders.processed.v1`.
- **Objetivos no funcionales prioritarios:** idempotencia ante re-entregas, resolución
  de concurrencia por `orderId`/`eventVersion` y tolerancia a fallos con reintentos.

El worker **no expone API HTTP**; su interfaz de entrada es Kafka y su interfaz de salida es
Kafka + MongoDB.

## 2. Arquitectura de Alto Nivel

```
                 ┌──────────────────────────────────────────────────────────────────┐
                 │                  docker-compose (order-network)                  │
                 │                                                                  │
  producer       │   ┌─────────────────────┐        ┌─────────────────────┐         │
  (pedido B2B)   │   │    products-api     │        │    clients-api      │         │
      │          │   │    (Go, 8082)       │        │  (NestJS, 3000)     │         │
      ▼          │   └─────────┬───────────┘        └─────────┬───────────┘         │  
 ┌────────┐      │             │ GET /products/{id}?market=   │ GET /clients/{id}   │
 │ Kafka  │      │             │                              │                     │
 │ broker │      │             └───────────────┬──────────────┘                     │
 └───┬────┘      │                             │                                    │
     │           │                    ┌────────▼─────────┐                          │
     │  orders.created.v1             │  order-processor │   (Java / Spring Boot)   │
     │           │                    │  hexagonal       │                          │
     │           │                    │  - listener      │                          │
     │           │                    │  - use case      │                          │
     │           │                    │  - adapters out  │                          │
     ▼           │                    └───┬─────────┬────┘                          │
┌───────────┐    │                        │         │                               │
│ consumers │◄───┼────────────────────────┘         │                               │
│ orders.   │    │  orders.processed.v1             │persistencia                   │
│processed. │    │                                  ▼                               │
│   v1      │    │                          ┌─────────────┐   ┌──────────────┐      │
└───────────┘    │                          │   MongoDB   │   │ orders.proc. │      │
                 │                          │             │   │ essed.v1     │      │
                 │                          └─────────────┘   │              │      │
                 │                                            └──────────────┘      │
                 └──────────────────────────────────────────────────────────────────┘
```

### Componentes y decisiones de diseño

| Componente | Stack | Rol |
| --- | --- | --- |
| `order-processor` | Java 21, Spring Boot 3.5, spring-kafka, spring-data-mongodb | Worker. Arquitectura hexagonal: dominio puro sin Spring/Kafka/Mongo; casos de uso orquestan puertos; adaptadores (in: Kafka listener; out: Mongo, HTTP clients, Kafka producer). |
| `products-api` | Go 1.27, net/http, mongo-driver v2 | Catálogo por mercado (contrato 5.C). In-memory para arranque sin credenciales + Mongo con seed. |
| `clients-api` | NestJS 12, Express, Vitest | Clientes (contrato 5.B). Repositorio en memoria con seed inicial. |
| Infra | Docker Compose | Kafka + Zookeeper, MongoDB, Kafdrop (9000), Mongo Express (8081). |
| Observabilidad | Docker Compose | Grafana + loki + promtail |


## 3. Flujo Principal (feliz)

1. Un productor publica `orders.created.v1` con clave `orderId` (partición por pedido).
2. El `@KafkaListener` deserializa el payload JSON con `OrderEventJsonReader` (estricto en
   `quantity` entero, sin desconocer campos desconocidos).
3. Los mensajes que no se pueden resolver se derivan a la **DLT** `orders.processing.dlt` (ver §4):
   un payload no parseable lanza `ContractViolationException(DESERIALIZATION)` y una violación del
   contrato 5.A (`OrderContractService`) lanza `ContractViolationException(CONTRACT_VIOLATION)`; ambos
   quedan marcados como **no reintentables** y el manejador de errores los publica en la DLT con los
   headers de metadatos de la sección 7.
4. El listener construye `ProcessOrderCommand` y llama a `OrderProcessingService.processOrder`:
   - **Idempotencia (7.1):** si `eventId` ya fue procesado, no recalcula; reenvía el resultado si el
     evento vigente es el mismo (at-least-once del lado de salida).
   - **Versión obsoleta (7.3):** si el pedido ya existe con `eventVersion` mayor, el evento se ignora.
   - **Cliente:** `GET /clients/{clientId}`. 404 → `REJECTED` con razón; transitorio → propaga para
     reintento con backoff; otras respuestas definitivas → `TECHNICAL_FAILURE`.
   - **Productos:** `GET /products/{productId}?market=` por cada línea; clave sin correspondencia →
     `PRODUCT_NOT_FOUND`.
   - **Elegibilidad (6.1):** cliente `ACTIVE`, market del cliente == market del pedido, todos los
     productos `ACTIVE`; si falla alguna → `REJECTED` con razón explícita.
   - **Cálculo (6.2–6.4):** `OrderCalculatorService` con `BigDecimal`, `HALF_UP`, 2 decimales por
     línea y totales = suma de líneas ya redondeadas.
5. `OrderRepositoryAdapter.save` persiste el resultado (ver §4) y, si el evento realmente ganó la
   versión concurrente, `EventPublisherAdapter` publica `orders.processed.v1` (5.D) de forma
   **bloqueante**.
6. El offset de Kafka se confirma (`ack-mode: record`): solo se realiza commit si el listener no
   lanzó excepción.

## 4. Flujos Alternativos y Matriz de Errores (§7)

| Código HTTP interno | Clasificación | Acción del worker |
| --- | --- | --- |
| `404` | Recurso inexistente | Definitivo → `REJECTED` persistido + publicado |
| `429` | Rate limit | Transitorio → reintento con backoff exponencial (3 intentos) |
| `500`, `502`, `503` | Error interno | Transitorio → reintento con backoff exponencial |
| Timeout / red | Falla de red | Transitorio → reintento con backoff exponencial |
| Otra respuesta no contemplada | Fuera de contrato | Definitivo → `TECHNICAL_FAILURE` persistido + publicado |

- **Backoff (Kafka):** `ExponentialBackOffWithMaxRetries(3)` (intervalo inicial 1 s, multiplicador 2).
- **Backoff (HTTP):** `@Retry` de Resilience4j por adaptador (`resilience4j.retry.instances.clients-api`
  / `products-api`; 3 intentos, 500 ms ×2 hasta 4 s) reintenta **solo** fallos transitorios
  (`RetryableExternalServicePredicate`, que respeta la matriz anterior); el `@RateLimiter` (5/s)
  convierte su rechazo en `429` local transitorio que el `@Retry` exterior reintenta.
- **Fallo transitorio agotado:** el recoverer (`DltPublishingRecoverer`) publica el mensaje original
  en la DLT `orders.processing.dlt` con `errorCategory=RETRIES_EXHAUSTED`, la causa raíz resumida y el
  número de intentos; no se persiste estado final.
- **TECHNICAL_FAILURE:** un error externo **definitivo** (401/2xx malformado) no se reintenta; se
  persiste y se publica en `orders.processed.v1` para dar trazabilidad (totals en cero en el evento
  de salida).
- **No parseable / violación de contrato:** los mensajes que no son JSON (ni `quantity` entero en una
  línea) o que incumplen 5.A lanzan `ContractViolationException` y van **directo a la DLT** sin
  reintentos, con `errorCategory` `DESERIALIZATION` o `CONTRACT_VIOLATION`.
- **DLT (`orders.processing.dlt`):** headers `orderId`, `eventId`, `errorCategory`, `summaryCause`,
  `attemptCount`, `timestamp`, `component`; el payload original se reenvía sin modificar. La
  publicación es *best-effort* para no crear un bucle de reentrega (ver ADR-004).
- **Excepción fiscal (6.2):** cliente `EXEMPT` → tasa efectiva 0 %.
- **Rechazo por catálogo:** `PRODUCT_DISCONTINUED`, `PRODUCT_NOT_FOUND`,
  `CLIENT_NOT_FOUND`, `CLIENT_BLOCKED`, `MARKET_MISMATCH` son razones de `REJECTED`.

## 5. Concurrencia (resumen; detalle en ADR-001)

- Inserción atómica por `orderId` (`_id`); la carrera de inserción produce `DuplicateKeyException`.
- Comparación y asignación atómica (`findAndModify`) con filtro `eventVersion < <entrante>`.
- El caso de uso solo publica si el `eventId` guardado es el del evento procesado (evita publicar
  resultados que no le corresponden al evento ganador).
- Clave de Kafka = `orderId` como complemento (serialización por partición) pero no como única
  garantía.

## 6. Consistencia Mongo–Kafka (resumen; detalle en ADR-002)

- Persistir y luego publicar de forma **bloqueante** (no fire-and-forget).
- Si la publicación falla tras persistir, la re-entrega idempotente del mismo `eventId` la sanja
  repitiendo el evento del estado actual persistido (at-least-once).
- Los consumidores de `orders.processed.v1` deduplican por `sourceEventId`.
- Outbox transaccional se documenta como evolución futura (ver ADR-002).

## 7. Topología de Despliegue Local

- `docker-compose up` levanta infraestructura (Kafka, Zookeeper, Mongo, UIs) por defecto.
- Perfil `full` agrega los tres servicios: `clients-api:3000`, `products-api:8082`,
  `order-processor` (sin puerto HTTP, consume del broker interno `kafka:29092`).
- Sin Docker, cada servicio corre en local con seeds en memoria / `localhost` para Mongo y Kafka.

## 8. Decisiones Ajadas (registradas en `implementation-notes.md`)

- Índice `{eventId: 1}` no se crea en arranque (se delega su creación explícita).
- Los seeds de `clients-api` y `products-api` son estáticos (en memoria y Mongo deben mantenerse en
  sincronía).
- La publicación de un estado final es bloqueante y, si Kafka vuelve a fallar, la re-entrega
  idempotente del mismo evento repite el estado persistido (el mensaje no se pierde: su offset no se
  confirma hasta persistir y publicar).
