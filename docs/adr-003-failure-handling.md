# ADR-003: Manejo de Errores sin DLT (estado final como contrato)

- **Estado:** Aceptado
- **Fecha:** 2026-10-07
- **Decisores:** líder técnico del worker (`order-processor`)
- **Relacionados:** ADR-001 (concurrencia), ADR-002 (consistencia Mongo-Kafka)

## Contexto

La sección 7 de `Expecificaciones.md` definía una DLT (`orders.processing.dlt`) como destino de los
mensajes que no pueden procesarse, con 7 headers de metadatos. Durante la implementación de la
fase 2 se evaluó el costo real de esa decisión:

- El alcance exige que **cada pedido llegue a un estado final**: `APPROVED`, `REJECTED` o
  `TECHNICAL_FAILURE`. Un mensaje en la DLT rompe esa invariante: existe sin estado final.
- La DLT crea un **segundo camino de salida** (`publishToDLT`) que compite con
  {`orders.processed.v1`}, duplica el código del puerto/adaptador de salida y añade sus propios
  modos de fallo (¿qué pasa si la publicación a DLT también falla? → best-effort, loguear y esperar
  la re-entrega).
- Categorizar un mensaje como "no recuperable" y desviarlo fuera del flow duplica la
  trazabilidad: para saber el destino de un pedido hay que mirar el tópico de salida **y** la DLT.

## Decisión

**No existe DLT.** Todo mensaje que entra al worker termina persistido en MongoDB y publicado en
`orders.processed.v1` con su estado final, resolviendo su clasificación en una de dos capas:

1. **En el listener / caso de uso (sin reintentos):**
   - Payload que no puede deserializarse (JSON inválido, `quantity` no entero en una línea) →
     `OrderCreatedListener.recordUnprocessable(...)` con razón `DESERIALIZATION: <detalle>`.
   - Violación de las 7 reglas del contrato 5.A → `OrderContractService.validate`
     (`OrderProcessingService`) → estado `REJECTED`, razón `CONTRACT_VIOLATION: <reglas>`.
2. **Al agotar los reintentos con backoff** (429/5xx/timeout o fallo interno repetido) →
   `ProcessingFailureRecoverer.accept(...)`: payload parseable → `recordTechnicalFailure(...)`
   (razón `TECHNICAL_FAILURE: <causa raíz>`); payload no parseable → `recordUnprocessable(...)`
   (razón `RETRIES_EXHAUSTED: <causa raíz>`).

Los tres estados se escriben con el esquema actual del documento (`OrderDocument`) a través del
mismo caso de uso y se publican con el contrato 5.D intacto (con `market`/`currency` en `null` en el
JSON cuando el pedido es parcial, y `totals` en cero en `TECHNICAL_FAILURE`).

### Salvage de identificadores para mensajes no procesables

Un payload que no se puede leer no tiene un `orderId`/`eventId` confiables, pero el documento de
resultado necesita id. Para que la re-entrega de Kafka (mismo tópico/partición/offset) reemplace el
mismo documento —y no cree moscas duplicadas— se resuelve el orden:

1. `orderId` del payload (lectura tolerante con `OrderEventJsonReader.readTextField`).
2. Si falta, la clave del registro de Kafka.
3. Si falta, id sintético determinístico `orderId = eventId = <topic>-<partition>-<offset>`.

### Versionado sobre mensajes no procesables

`recordUnprocessable` persiste el documento con `eventVersion = 0` y sin comparación CAS de
versión. Consecuencia aprovechada: cualquier evento válido posterior del mismo `orderId`
(`eventVersion >= 1`) gana la actualización por el CAS 7.2/7.3 y **reemplaza** al documento no
procesable, dejando el estado final real. Un `orderId` parcial que nunca recibe un evento válido
queda como `REJECTED` no procesable: trazable por el payload crudo en `sourcePayload`.

## Consecuencias

**Positivas**

- Invariante simple y verificable: **no hay mensaje que no termine persistido y publicado** con su
  estado final (Refuerza ADR-002).
- Un solo puerto de salida (`EventPublisherPort.publishOrderProcessed`): menos superficie, menos
  modos de fallo, tests unitarios más simples.
- Trazabilidad en un solo lugar: Mongo (`orders`) + `orders.processed.v1`. El payload original
  problemático se conserva en `Order.sourcePayload` para reproducir incidentes sin volver al broker.
- Los tests de integración validan el comportamiento end-to-end contra un broker embebido y Mongo
  de testcontainers (casos `REJECTED` y `DESERIALIZATION` publicados en `orders.processed.v1`).

**Negativas / riesgos**

- Un suscriptor de `orders.created.v1` que vea un `REJECTED` por deserialización no recibe el
  payload original en el evento de salida (el contrato 5.D no lo incluye); se recupera del
  documento Mongo (`sourcePayload`) si la auditoría lo necesita.
- El `eventVersion = 0` de los parciales es un valor fuera del contrato de entrada (que exige ≥ 1);
  es un detalle interno del documento de salida Mongo, no del evento.
- Si un payload ilegible llega repetido por una clave que luego usa un pedido legítimo, el CAS lo
  sobrescribirá: comportamiento deseado (el evento válido es el estado final), documentado aquí
  para que no se lea como pérdida de datos.

## Alternativas consideradas

1. **DLT con 7 headers (§7 original).** Descartada: duplica el camino de salida, rompe la
   invariante de estado final y añade un punto extra de fallo (ver Contexto).
2. **Reintentos infinitos (sin recuperación).** Descartada: mensajes envenenados bloquearían el
   consumo de la partición y Kafka expulsaría al consumidor; el backoff con aterrizaje en
   `TECHNICAL_FAILURE` es el punto medio.
3. **Repositorio de dead-letter propio en Mongo + revisor manual.** Aceptable a futuro si el
   volumen de mensajes no procesables justifica un área de inspección dedicada; hoy `sourcePayload`
   + `reason` en el documento cubren la auditoría. (Trigger documentado en
   `implementation-notes.md`.)

## Verificación

- `OrderCreatedListenerTest` / `ProcessingFailureRecovererTest`: paths de deserialización,
  salvage de ids e id sintético; `OrderContractServiceTest` cubre las 7 reglas de 5.A.
- `KafkaConsumerConfigTest`: los únicos fallos que van al recoverer son los agotados por backoff
  (no hay "sin reintentos" a nivel de manejador).
- `OrderCreatedKafkaFlowIT` (with embedded Kafka + MongoDB testcontainers): payload fuera de
  contrato → `REJECTED` publicado en `orders.processed.v1`; payload no JSON → `REJECTED` con razón
  `DESERIALIZATION` y `sourcePayload` presente.
- Suite completa: 104 tests, 0 fallos.