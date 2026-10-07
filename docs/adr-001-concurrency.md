# ADR-001 — Concurrencia y procesamiento del agregado `Order`

- **Estado:** Aceptada e implementada
- **Fecha:** Oct 2026
- **Decisores:** Tech lead (orden) con el equipo de 4
- **Relación:** Sección 7 de `Expecificaciones.md` ("Escenarios de Concurrencia a Manejar")

## Contexto

El consumidor de `orders.created.v1` opera en un tópico con al menos-una vez de entrega y deja
entrever tres escenarios que el repositorio y el caso de uso deben resolver **sin coordenar entre
instancias** (el worker escala horizontalmente):

1. **7.1 — Re-entrega del mismo `eventId`:** un `commit` fallido o la entrega al menos-una vez
   reenvía el mismo evento. No debe duplicar efectos ni cambiar el resultado persistido.
2. **7.2 — Eventos concurrentes con el mismo `orderId` y `eventVersion`:** dos réplicas pueden
   recibir dos versiones del mismo pedido casi a la vez. Solo una debe ganar y el resultado ganador
   debe ser el del evento **más reciente**.
3. **7.3 — Versión antigua recibida después de una más reciente:** el evento debe **ignorarse**,
   preservando el resultado más nuevo.

Restricción técnica fuerte: **la transacción del dominio (persistencia) y la de salida Kafka no
pueden ser atómicas entre sí** (no existe XA entre Mongo y Kafka; introducirlo destruiría la
simplicidad operativa local). La respuesta correcta no es "eliminar la ventana", sino resolver el
**ganador** en el almacén y publicar siempre **lo que quedó persistido**.

## Opciones consideradas

### A. Bloqueo pesimista de fila en Mongo (findAndModify con lock propio / `$isolated`)

- **Pro:** simple de entender; **Con:** Mongo no ofrece locks pesimistas portátiles; serializa todos
  los pedidos en un mismo shard; penaliza a los eventos no contendientes. Descartada.

### B. Optimistic concurrency control over `eventVersion` (elección)

- Inserción atómica por clave primaria `orderId` (`_id`) + `findAndModify` con filtro
  `eventVersion < <entrante>` como *compare-and-set* atómico del servidor Mongo. Ver abajo.

### C. Lock distribuido externo (Redis/Redlock)

- **Pro:** patrón conocido. **Con:** **nueva dependencia infraestructural y operativa** (el stack
  obligatorio no la incluye), riesgo de particiones/tiempos de vida, supervivencia fuera de la
  garantía del broker. No aporta nada que B no resuelva bajo el modelo de la sección 6 del spec.
  **Descartada** — coste sin beneficio para un solo worker de consumo.

### D. Serialización por partición Kafka (clave = `orderId`)

- **Pro:** si el productor usa `orderId` como clave, un mismo pedido llega secuencialmente por
  partición; **Con:** el spec solo *recomienda* la clave; un consumidor con más de una partición por
  `orderId`, un rebalance o un productor descuidado rompen la unicidad de ejecución. Se adopta
  **como capa complementaria** (reduce contención), nunca como única garantía.

### E. Event-sourcing / ledger de eventos (`order-events` + proyección)

- **Pro:** trazabilidad perfecta; **Con:** sobre-ingeniería para el alcance (un solo estado final por
  pedido), más colecciones, más migraciones. Se registra como evolución futura opcional, no como
  decisión inicial. **Descartada para esta iteración.**

## Decisión

`OrderRepositoryAdapter` implementa **Control de Versión Optimista por `eventVersion` sobre `_id`**:

```
save(order):
  1) try insert(_id = orderId)            # primera versión que llegue gana la inserción
       → Éxito: devolver order
       → DuplicateKeyException: otra réplica/versión ya insertó → pasar a (2)
  2) findAndModify( { _id: orderId, eventVersion < entrante },
                    { $set: campos del evento entrante } )
       → devuelve "new": si actualizó, ese documento es el nuevo ganador
       → devuelve null: una versión igual o más reciente ganó; leer el documento guardado
  3) El caso de uso compara eventId guardado vs eventId del evento → solo publica si coinciden
```

Reglas resultantes:

- **7.1 (idempotencia):** `existsByEventId(eventId)` corta antes de recircular. Si el evento vigente
  es el mismo, se **republica** lo guardado (ver ADR-002); el estado no cambia.
- **7.2 (concurrencia):** la inserción falla para el perdedor y el CAS solo aplica versiones
  estrictamente mayores. El perdedor **no publica** (el CAS o la comparación posterior lo impiden).
- **7.3 (obsoleto):** `findAndModify` no toca nada (el filtro no casa) y el listener devuelve lo
  persistido sin publicar.

Detalles operativos:

- `receivedAt` conserva la **primera** recepción (no se sobreescribe en el CAS).
- El `timestamp` de Kafka se usa como componente del evento; la resolución del ganador es
  **idempotente por valor de datos**, no por orden de llegada al reloj.
- No se crean índices en arranque: el índice `{eventId: 1}` necesario para `existsByEventId` es
  **deuda técnica** (ver `implementation-notes.md`); el índice único real es `_id`.

## Consecuencias

**Positivas**

- Correcto bajo escalado horizontal: ninguna dependencia externa de coordinación.
- El resultado publicado refleja *siempre* lo que quedó en Mongo (invariante: publicación = estado).
- Los reintentos de Kafka son baratos: un evento perdedor convergente se comporta como 7.1.

**Negativas / riesgos**

- La discriminación del ganador vive en el repositorio: cualquier futuro repositorio (p. ej.
  PostgreSQL) debe emular el CAS o el rediseño de la decisión se reabre.
- Sin el índice `{eventId: 1}`, el desempeño de 7.1 degrada con el volumen: mitigado creándolo
  explícitamente en instancias productivas (pendiente, ver deuda técnica).
- Una versión entre `null` y `0` se considera obsoleta si el pedido ya existe con versión mayor;
  actualmente `eventVersion` es `1` constante (evento salida, 5.D) y `>= 1` de entrada (5.A).

## Alternativa aceptada como plan B sin decisión

Si en producción la presión de escritura hiciera inviable el CAS sobre una colección única, se
evaluaría particionar `orders` por mercado manteniendo la misma semántica de CAS por `orderId`.