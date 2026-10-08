# ADR-002 — Consistencia entre MongoDB y Kafka para `orders.processed.v1`

- **Estado:** Aceptada e implementada
- **Fecha:** Oct 2026
- **Decisores:** Tech lead (orden) con el equipo de 4
- **Relación:** Secciones 5.D y 7 de `Expecificaciones.md` (evento de salida e idempotencia)

## Contexto

El worker persiste el resultado de un pedido en MongoDB (`orders_db`) y **también** publica el
evento `orders.processed.v1` en Kafka (contrato 5.D). Los consumidores de salida (facturación,
notificaciones, BI) asumen que un evento publicado corresponde a un resultado persistido y viceversa.

El problema: **Mongo y Kafka no comparten transacción.** No hay XA entre ambos; `spring-kafka`
permite transacciones del productor ("exactamente una vez") pero eso **no alcanza a Mongo**, y
relajar los ack del consumidor para sincronizar los dos commits es frágil y opaco. Toda solución se
mueve entre dos fallos simétricos:

1. **Evento perdido:** persistimos OK pero la publicación falla (broker caído, red, límite de
   reintentos) y nadie vuelve a contar la historia → consumidor de salida no se entera del pedido.
2. **Evento fantasma:** publicamos algo que la persistencia terminó descartando (p. ej. una versión
   concurrente perdió) → consumidor procesa una realidad inexistente.

El diseño busca cumplir: **"siempre que se publique, el consumidor puede reconstruir el estado
persistido; y ninguna re-entrega duplica efectos".**

## Opciones consideradas

| Opción | Ventajas | Costes / riesgos |
| --- | --- | --- |
| **A. Fire-and-forget** (persistir, publicar asíncronamente sin bloqueo) | Máxima throughput de consumo | Pérdida silenciosa de eventos; ninguna garantía semántica. **Descartada.** |
| **B. Persistir + publicar bloqueante** (elección) | El offset solo se confirma si la cadena completa terminó; la re-entrega idempotente republica el estado vigente | Ata throughput al RTT del broker; depende de disponibilidad de Kafka para *completar* (no para persistir). |
| **C. Outbox transaccional** (colección `outbox` + publicador/poller) | Zero-loss por diseño; desacopla Mongo de la disponibilidad de Kafka | Colección extra, idempotencia del publicador, reintentos del publicador, mayor superficie de código y operativa. Mejor **cuando** haya varios consumidores o sea inaceptable bloquear el consumidor. |
| **D. Kafka exactly-once (EOS) `isolation.level=read_committed`** | Garantías fortes en Kafka | No alcanza a Mongo: la unidad transaccional se rompe para el estado; bloquea el mensaje hasta commit del producer; sobre-promete para este caso. **Descartada como garantía única.** |
| **E. IDempotencia dirigida (dedup en consumidor de salida)** | Complemento barato: `sourceEventId` como clave de deduplicación | No evita pérdidas del lado emisor; se requiere de todos modos. **Se adopta como pacto de contrato 5.D.** |

## Decisión

Se adopta **B + E, con arquitectura "reintento por re-entrega"** y el caso de uso como árbitro:

```
processOrder(evento):
  if existsByEventId(eventId):              # 7.1 re-entrega (idempotencia)
      if estado.guardado.eventId == eventId:
          publicar(estado.guardado)          # sana publicaciones fallidas previas
      return estado.guardado

  ... computar resultado ...               # validación, elegibilidad, cálculo
  estadoGuardado = repo.save(resultado)     # commit a Mongo (ADRS-001 CAS)
  if evento ganó la versión (eventId coincide):
      eventPublisher.publish(eventoSalida)  # bloqueante: falla ⇒ excepción
      # si esta publicación lanza, el listener propaga ⇒ NO hay commit del offset
      # ⇒ Kafka re-entrega el mismo evento ⇒ la rama 7.1 lo republica.
      return estadoGuardado
```

Garantías que se obtienen:

- **Nunca se confirma el offset de un evento cuyo evento de salida no se publicó.** El lector de
  Kafka (commit por registro, `ack-mode: record`) fuerza `ALBD-once`: cada mensaje se procesa hasta
  publicar o registrar su estado definitivo (backoff → `TECHNICAL_FAILURE`).
- **Nunca se publica un resultado descartado:** el caso de uso interroga el `eventId` realmente
  persistido (ADR-001) antes de publicar.
- **La reentrega no duplica efectos:** el reproceso taller 7.1 republica *el estado vigente*, y los
  consumidores de `orders.processed.v1` deduplican por `sourceEventId` (contrato 5.D).
- **Los fallos definitivos también se publican:** intentos agotados del mismo evento con fallo
  definitivo → se persiste `TECHNICAL_FAILURE` (cierre contable) y se publica su evento; los
  payloads ilegibles se registran como `REJECTED` no procesable conservando el payload crudo en
  `sourcePayload` (ADR-003).

### ¿Por qué no outbox (C) todavía?

El outbox resuelve pérdidas sin bloquear el consumo, pero:

- Con **un solo consumidor** de salida y **una instancia** de worker local, el bloqueo al Kafka es
  de pocos milisegundos y el volumen de pedidos B2B no lo hace crítico.
- El outbox exige limpieza, publicador confiable y conexión a la misma transacción Mongo, lo que
  ataría el worker a funciones de Mongo (transacciones replicadas) que hoy no son necesarias.
- La redundancia con la idempotencia por `eventId` (re-entrega = replays) ya cubre la pérdida de
  publicación siempre que Kafka siga disponible, que es el caso de uso real.

**Trigger para reevaluar C:** más de un consumidor de salida, volumen/rendimiento que vuelva el RTT
del broker inaceptable, o requisito de cero pérdidas ante indisponibilidad prolongada de Kafka.

## Consecuencias

**Positivas**

- Invariante verificable: `outbox` no existe, pero **todo evento publicado existe persistido**, y
  **todo resultado persistido tiene su evento emitido** (eventualmente, vía re-entrega o
  re-publicación del estado persistido).
- Tests deterministas: el orden de efectos (persistir → publicar) se puede probar con mocks sin
  infraestructura.

**Negativas / riesgos**

- Throughput de consumo limitado por el RTT del broker: aceptable para el dominio.
- Si Kafka queda caído **más** tiempo que la retención del tópico de entrada, podría perder eventos
  no procesados (cubierto operativamente por el volumen de re-entrega/deduplicación, no por diseño).
- El punto crítico es la publicación bloqueante final: si Kafka vuelve a fallar al publicar el
  estado, se loguea el fallo y el mensaje original queda sin confirmar en el tópico de entrada (el
  payload nunca se pierde en origen gracias al commit por registro); la re-entrega repite el estado
  persistido.
- El pacto de deduplicación por `sourceEventId` pasa a ser un **requisito de contrato** de los
  consumidores de salida (documentado en el contrato 5.D).

## Estrategia de verificación

- Tests de contexto de Spring con Kafka embebido (o Testcontainers en CI): se simula fallo de
  publicación → se asegura 0 commits → se re-entrega el mensaje → se verifica re-publicación
  idempotente y 1 solo evento de dedup sobre una aserción de `sourceEventId`.
- La IT de Mongo (`OrderRepositoryAdapterIT`, Testcontainers) valida el CAS y la carrera de
  inserción.