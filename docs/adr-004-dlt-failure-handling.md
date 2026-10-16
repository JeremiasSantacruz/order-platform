# ADR-004 — Tópico de mensajes fallidos (`orders.processing.dlt`)

- **Estado:** Aceptada e implementada (supersede a ADR-003)
- **Fecha:** Oct 2026
- **Decisores:** Tech lead (orden) con el equipo de 4
- **Relación:** Secciones 5.A y 7 de `Expecificaciones.md`; supersede ADR-003; condiciona ADR-002
  (consistencia Mongo-Kafka)

## Contexto

ADR-003 resolvió el manejo de errores sin DLT: **todo** mensaje, incluso el no procesable, terminaba
como un estado final persistido en MongoDB y publicado en `orders.processed.v1`
(`REJECTED` para payload ilegible y violaciones de contrato, `TECHNICAL_FAILURE` al agotar
reintentos). Ese diseño tenía la ventaja de un único invariante ("todo evento publicado existe
persistido") pero mezclaba dos cosas muy distintas en el tópico de salida:

1. **Resultados de negocio** que los consumidores de `orders.processed.v1` (facturación,
   notificaciones, BI) deben interpretar.
2. **Mensajes técnicamente irrecuperables** (JSON roto, contrato incumplido, agotamiento de
   reintentos) que esos consumidores no deberían procesar como pedidos reales.

El costo aparecía en la operación: un payload corrupto se contabilizaba como un pedido `REJECTED`,
sin separación de canal y sin los metadatos técnicos (categoría de error, intentos, componente) que
un equipo de guardia necesita para diagnosticar. La sección 7 de `Expecificaciones.md` siempre previó
un **Dead Letter Topic** con headers de metadatos; esta ADR retoma esa especificación.

## Opciones consideradas

| Opción | Ventajas | Costes / riesgos |
| --- | --- | --- |
| **A. Mantener ADR-003** (estado final `REJECTED`/`TECHNICAL_FAILURE` para todo, sin DLT) | Un solo invariante; sin infraestructura extra; un solo tópico que observar | Confunde resultado de negocio con fallo técnico; sin metadatos de diagnóstico; un payload envenenado ensucia el conteo de negocio. Ya implementado. **Descartada.** |
| **B. DLT real `orders.processing.dlt` con headers 5.A/7** (elección) | Separa el canal de fallos del de resultados; metadatos de diagnóstico (`errorCategory`, `summaryCause`, `attemptCount`, `component`); el payload original queda disponible para reproceso | Segundo tópico y un recoverer; hay que garantizar que publicar en la DLT sea *best-effort* para no crear un poison-pill; se rompe el invariante "todo mensaje queda persistido". |
| **C. Cuarentena en MongoDB** (colección `dead_letters`) | Consultable con las herramientas existentes; sin depende de Kafka para el reproceso | Acopla el manejo de fallos al almacén de negocio; el reproceso necesita una herramienta a medida; no es el diseño pedido por la especificación. **Descartada.** |

## Decisión

Se adopta **B**: los mensajes que no pueden procesarse se publican en la DLT
`orders.processing.dlt` con siete headers de metadatos, y **no** se persisten como estado final.

Clasificación en la entrada (adaptador Kafka):

```
payload no parseable          ⇒ ContractViolationException(DESERIALIZATION)      ┐
violación del contrato (5.A)  ⇒ ContractViolationException(CONTRACT_VIOLATION)   ├─ no reintentable ⇒ DLT directa
                                                                                 ┘
429 / 5xx / timeout / interno ⇒ reintento con backoff exponencial                ┐
                                                                                 ├─ al agotar ⇒ DLT (RETRIES_EXHAUSTED)
                                                                                 ┘
rechazo de negocio (404, BLOCKED, DISCONTINUED, mercado) ⇒ REJECTED persistido + orders.processed.v1
error externo definitivo (401, 2xx malformado)           ⇒ TECHNICAL_FAILURE persistido + orders.processed.v1
```

Headers de la DLT (sección 7):

| Header | Origen |
| --- | --- |
| `orderId` | Payload; si es ilegible, la clave del registro |
| `eventId` | Payload; si es ilegible, `unknown` |
| `errorCategory` | `CONTRACT_VIOLATION`, `DESERIALIZATION` o `RETRIES_EXHAUSTED` |
| `summaryCause` | Causa raíz resumida (reglas incumplidas o excepción) |
| `attemptCount` | Cabecera `KafkaHeaders.DELIVERY_ATTEMPT` (1 si no hubo reintento) |
| `timestamp` | Momento del descarte (ISO-8601) |
| `component` | `spring.application.name` (`order-processor`) |

Reglas de implementación:

- **Un solo camino de DLT:** el recoverer de entrada (`DltPublishingRecoverer`) traduce el fallo a
  `DeadLetterInfo` y delega la construcción/publicación del registro en el adaptador de salida
  (`EventPublisherPort.publishToDLT`). No hay un segundo productor.
- **La publicación en la DLT es best-effort:** si el broker rechaza la publicación, se registra el
  fallo y se confirma el offset igualmente; lanzar dentro del recoverer provocaría un bucle de
  reentrega con el broker caído (poison-pill).
- **El payload original se reenvía intacto** en el valor del registro, para no depender de reproducir
  el evento al investigar el incidente.
- **Reintentos:** `DefaultErrorHandler` con `ExponentialBackOffWithMaxRetries(max-retries)` y
  `ContractViolationException` en `addNotRetryableExceptions` (fallo inmediato, sin backoff).

## Consecuencias

**Positivas**

- El tópico `orders.processed.v1` vuelve a contener **solo** resultados de negocio interpretables.
- El equipo de guardia tiene los metadatos técnicos por mensaje para diagnosticar y reprocesar.
- La especificación §7 queda cumplida sin acoplar el manejo de fallos al almacén de negocio.

**Negativas / riesgos**

- **Se rompe el invariante de ADR-003:** un mensaje derivado a la DLT no tiene estado final en
  MongoDB. La trazabilidad se parte entre `orders` (resultados) y `orders.processing.dlt` (fallos).
  Mitigación: `orderId`/`eventId` como headers y el payload original permiten correlacionar.
- **Poison-pill:** si la publicación en la DLT lanzara, la reentrega podría ciclar. Mitigado por el
  manejo best-effort del adaptador.
- El error externo **definitivo** (401/2xx malformado) mantiene el comportamiento de ADR-003
  (`TECHNICAL_FAILURE` persistido y publicado); el "error no recuperable" que va a la DLT es el de
  contrato/deserialización y el de reintentos agotados.

## Estrategia de verificación

- `DltPublishingRecovererTest`: mapeo de cada `errorCategory`, `attemptCount` desde la cabecera de
  intento y fallback, `orderId` desde clave, `eventId=unknown` y payload ilegible.
- `EventPublisherAdapterTest`: los 7 headers y el payload crudo en el registro de la DLT.
- `KafkaConsumerConfigTest`: violación de contrato se recupera sin reintentos; el fallo transitorio
  espera el backoff y solo va al recoverer al agotar `max-retries`.
- `OrderCreatedKafkaFlowIT` (Kafka embebido + Testcontainers): payload fuera de contrato y payload no
  JSON llegan a `orders.processing.dlt` con sus headers y **sin** documento en MongoDB; un rechazo de
  negocio sigue publicando `REJECTED` en `orders.processed.v1`.
