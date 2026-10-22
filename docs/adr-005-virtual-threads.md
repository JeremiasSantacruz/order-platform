# ADR-005 — Hilos virtuales en el worker de pedidos

- **Estado:** Aceptada e implementada
- **Fecha:** Oct 2026
- **Decisores:** Tech lead (orden) con el equipo de 4
- **Relación:** Secciones 5.B, 5.C y 7 de `Expecificaciones.md`; condiciona ADR-001 (concurrencia),
  ADR-002 (consistencia Mongo-Kafka) y ADR-004 (DLT). No cambia contratos ni semántica de negocio.

## Contexto

`order-processor` es un worker sin servidor HTTP: todo su trabajo es **I/O bloqueante** sobre un
único hilo de consumo por contenedor de Kafka:

1. `@KafkaListener` recibe el registro (`ack-mode: record`).
2. `RestClient` consulta Clients API y Products API (una llamada por cliente y una por producto).
3. `MongoTemplate` lee y hace el CAS por `eventVersion`.
4. `KafkaTemplate.send(...).get(...)` publica el resultado de forma bloqueante (ADR-002).

Con hilos de plataforma, la concurrencia se paga con memoria y cambios de contexto: cada hilo
bloqueado en red o disco ocupa un hilo del sistema operativo. Además, el lote de productos se
consultaba **secuencialmente**, por lo que la latencia de un pedido de N líneas era la suma de N
round-trips. El proyecto ya corre sobre JDK 21 (local, `Dockerfile` y toolchain de Gradle) y
Spring Boot 3.5 / Spring Kafka 3.3, que soportan hilos virtuales de forma nativa.

## Opciones consideradas

| Opción | Ventajas | Costes / riesgos |
| --- | --- | --- |
| **A. Ajustar pools de hilos de plataforma** | Cero cambios de modelo; familiar | Dimensionar pools para cargas I/O es frágil; cada hilo bloqueado cuesta ~1 MB de stack; el fan-out de productos sigue secuencial |
| **B. WebFlux + reactor-kafka + Mongo reactivo** | Multiplexación de I/O y backpressure explícita | No hay servidor HTTP que justifique WebFlux; `reactor-kafka` no está auto-configurado y obliga a reimplementar `DefaultErrorHandler`, DLT, `attemptCount`, commits y la publicación bloqueante de ADR-002; los aspectos de Resilience4j no aplican a `Mono`/`Flux`; el MDC no cruza operadores sin `context-propagation`. **Descartada.** |
| **C. Hilos virtuales (JDK 21)** | Modelo imperativo intacto (fácil de testear y de razonar); soporte oficial en Spring Boot (`spring.threads.virtual.enabled`) y en Spring Kafka; MDC/ThreadLocal siguen funcionando; permite fan-out barato por pedido | Requiere JDK 21; el *pinning* por `synchronized` en librerías de terceros debe vigilarse; no acelera CPU-bound |
| **D. `parallelStream` para consultar productos** | Una línea de código | Usa el `ForkJoinPool.commonPool` (hilos de plataforma compartidos); sin cota ni control de concurrencia; errores difíciles de correlacionar. **Descartada.** |

## Decisión

Se adopta **C**:

1. **Listener de Kafka:** `spring.threads.virtual.enabled=true`. Spring Boot detecta la propiedad
   (`@ConditionalOnThreading(VIRTUAL)`) y configura el contenedor con un `SimpleAsyncTaskExecutor`
   con `setVirtualThreads(true)`. No se modifica la concurrencia del contenedor (queda en 1).
2. **Fan-out de productos:** `ProductsClientAdapter.getProducts` crea un
   `Executors.newVirtualThreadPerTaskExecutor()` por lote y usa `invokeAll`, con un
   `Semaphore(MAX_CONCURRENT_REQUESTS = 5)` para acotar las peticiones en vuelo. El resultado se
   recompone en un `LinkedHashMap` respetando el orden de entrada.
3. **Transporte HTTP:** `OutboundServicesConfig` usa `JdkClientHttpRequestFactory` (`java.net.http`)
   en lugar de `HttpURLConnection`, para no exponer a los hilos portadores a *pinning* por
   `synchronized`. Se conservan los timeouts (`CLIENTS_*`/`PRODUCTS_*`), HTTP/1.1 y el seguimiento
   de redirecciones de GET.
4. **Resiliencia sin cambios de contrato:** `@Retry`/`@RateLimiter` siguen en los métodos del
   puerto. Al ejecutarse el GET individual fuera del proxy (auto-invocación desde el lote, igual que
   antes de paralelizar), el permiso del `RateLimiter` se consume **por lote**, no por producto; el
   `Semaphore` evita que un pedido con muchas líneas dispare todas las consultas a la vez.

Detalle de ejecución del lote:

```
getProducts(ids, market)                 [hilo virtual del listener Kafka]
  └─ newVirtualThreadPerTaskExecutor
       ├─ vthread 1 → Semaphore(5) → GET /products/PRD-001  ┐
       ├─ vthread 2 → Semaphore(5) → GET /products/PRD-008  ├─ invokeAll
       └─ vthread N → Semaphore(5) → GET /products/PRD-00N  ┘
  └─ recomposición en orden de entrada; se propaga la primera falla en ese orden
```

## Consecuencias

**Positivas**

- El hilo de consumo no ocupa un hilo de plataforma mientras espera HTTP, Mongo o Kafka.
- La latencia del lote de productos pasa de la suma de los round-trips a la del más lento (con cota 5).
- No se tocan contratos, tópicos, headers de DLT, semántica de idempotencia ni la publicación
  bloqueante: las ADR-001/002/004 siguen vigentes.
- MDC (`eventId`/`orderId`) y el logging JSON siguen funcionando porque el procesamiento de un
  mensaje ocurre en un solo hilo (virtual).

**Negativas / riesgos**

- **Pinning:** si se aumenta `spring.kafka.listener.concurrency` por encima de los núcleos
  disponibles, el soporte de Spring Kafka advierte que los hilos virtuales pueden quedar fijados a
  los portadores por los `synchronized` internos de los clientes. Se mantiene la concurrencia en 1 y
  se documenta el límite antes de escalarla.
- **Fan-out:** el lote ahora espera a que terminen todas las consultas antes de propagar una falla
  (antes cortaba en la primera). El resultado es determinista (primera falla en orden de entrada) y
  la cota de 5 acota el impacto.
- **Throttle por lote:** el `RateLimiter` (5/s) no se consume por producto; es el comportamiento que
  ya se daba por la auto-invocación. Si en el futuro se necesita un límite por request, debe
  moverse a un operador programático o a un cliente con `Retry`/`RateLimiter` propio.
- El razonamiento sobre *pinning* debe repetirse si se agregan librerías con `synchronized` en el
  camino caliente (p. ej. drivers).

## Estrategia de verificación

- `ProductsClientAdapterTest`: además de los casos de contrato/errores existentes, un test con una
  `ClientHttpRequestFactory` instrumentada comprueba que las consultas se solapan
  (`CountDownLatch`) y que corren en hilos virtuales (`Thread.isVirtual()`), conservando el orden
  del mapa.
- `OrderProcessorApplicationTests`: verifica que el `listenerTaskExecutor` del contenedor de Kafka
  ejecuta en un hilo virtual.
- Suite completa: `scripts/run-tests.sh order-processor` (o `./gradlew.bat test`).
- Verificación opcional de *pinning* en ejecución: `-Djdk.tracePinnedThreads=full`.

## Evolución futura

- **Structured Concurrency** (`StructuredTaskScope`) reemplazaría el `invokeAll` con cancelación y
  propagación de errores más explícitas cuando deje de ser *preview* (JDK 25+).
- **WebFlux/reactor-kafka** se reevalúa solo si el worker expone una API HTTP reactiva o si se
  migra el consumo completo a Reactor (proyecto con ADR propio); no se mezcla con el modelo actual.
