# Notas de Implementación — post-código y deuda técnica

> Complementa `architecture-proposal.md` y las ADR-001/002 con lo descubierto durante la
> implementación y lo que queda pendiente.

## Estado de la entrega

- **Order-processor:** 81 tests (unit + integración), **0 fallos**; 5 tests de integración Mongo
  con Testcontainers quedan **skipped** cuando no hay Docker disponible.
- **clients-api:** build OK; 10 tests unit + 4 e2e en verde.
- **products-api:** `go build ./...`, `go vet ./...`, `go test ./...` en verde (sin tests
  específicos todavía).
- Docker Compose (perfil `full`) alinea los tres servicios + infraestructura; no se validó en
  máquina por daemon Docker inactivo.

## Cambios de alineación de contrato (post-`domain`)

Descubrimos que ambas APIs candidatas desviaban del contrato de las secciones 5.B/5.C y se
corrigieron para que el worker consuma el wire exacto:

| Servicio | Desviación detectada | Corrección |
| --- | --- | --- |
| `clients-api` | Respuesta con `id` y `createdAt` (el contrato 5.B exige `clientId` y solo 6 campos) | Renombrado `id → clientId`; `createdAt` **eliminado del tipo de dominio** (crowd de wire type); specs y e2e actualizados. |
| `clients-api` | Sin cliente para el ejemplo 5.A→5.D | Seed `CLI-99821` (MX, ACTIVE, WHOLESALE, GENERAL) para hacer end-to-end el ejemplo de la spec. |
| `products-api` | Mercados `CC/MX/AR`, seeds AR/CL | Constantes `MX/CO/PE`; seeds (memoria y Mongo) remapeados a MX/CO/PE; `PRD-001` y `PRD-008` (STANDARD/ACTIVE) en MX para reproducir los totales 5.D exactos. |
| `products-api` | `market` ajeno al contrato consultado sin validación | `ErrInvalidMarket` valida miembros de `{MX, CO, PE}` → 400 rápido, clasificado por el worker como no reintentable. |

## Decisiones de implementación concretas

- **Dominio puro:** `OrderProcessingService` instancia `OrderCalculatorService` /
  `OrderEligibilityService` como campos (no beans Spring): la capa application es pseudo-ordenable
  y testeable sin contexto. Sin import Spring/Kafka/Mongo en `domain/` ni en `application/`
  (verificado con grep).
- **Evento de salida:** `eventVersion=1` constante; `eventId = <sourceEventId>-OUT`; `occurredAt`
  como ISO-8601; totals en cero cuando aplica `TECHNICAL_FAILURE`.
- **Lectura estricta de entrada:** `ObjectMapper` con `FAIL_ON_UNKNOWN_PROPERTIES=false` (payloads
  con campos adicionales no rompen el consumidor) pero `quantity` validado como entero estricto;
  reglas de entrada de 5.A implementadas en el lector-validator del listener.
- **HTTP salida:** `RestClient` con timeouts de conexión/lectura configurables
  (`CLIENTS_*`, `PRODUCTS_*`); respuestas "fuera de contrato" (p. ej. 2xx con shape inválido)
  derivan en error definitivo → `TECHNICAL_FAILURE` (no reintentar un 2xx inválido).
- **DLT unificada:** un solo recurso (`DltPublishingRecoverer` → `EventPublisherPort.publishToDLT`)
  construye los 7 headers del §7; `EventPublisherAdapter` es el único constructor de registros DLT.
- **spring-kafka 3.3.16:** el test del error-handler usa la API `handleOne(Exception, record,
  consumer, container)`.
- **Persistencia:** `OrderDocument` (record) mapea sin configuración extra via Spring Data Mongo;
  `BigDecimal ↔ Decimal128` automático. `FindAndModifyOptions` en `org.springframework.data.mongodb.core`.

## Deuda técnica registrada (para el backlog)

1. **Índice `{eventId: 1}` no se crea en arranque.** `existsByEventId` depende de él para
   escalar; generarlo exige conectarse a Mongo al levantar el contexto, lo que se evitó para que
   los tests de contexto corran sin infraestructura. → *Acción:* `mongosh`/script de migración en
   despliegues productivos.
2. **Sin outbox transaccional** (ver ADR-002). Trigger de reevaluación documentado.
3. **DLT best-effort:** si Kafka vuelve a fallar al publicar el mensaje fallido, se loguea y el
   mensaje original queda sin confirmar en `orders.created.v1` (Kafka reintentará). Alternativa
   futura: repositorio de dead-letter propio + revisor manual.
4. **Seeds estáticos duplicados** en `products-api` (memoria vs Mongo): deben mantenerse en
   sincronía a mano; un seed centralizado (fixtures compartidos) es mejora de marcador.
5. **`products-api` sin tests** propios (build/vet/test vacíos de aserciones): mérito de
   "checklist" cubierto por el worker; sugerido añadir `httptest` del `GetProductUseCase`.
6. **Los eventos DLT re-publicados por re-entrega pueden avanzar `attemptCount` de forma
   acumulativa** (header `DELIVERY_ATTEMPT` real vs `max-retries` configurado): aceptado, ya que la
   cabecera declara el intento global.
7. **`eventVersion` de entrada vs salida:** se acepta `eventVersion` como entrada (≥ 1) y se emiten
   con `1` de salida; no hay versionado de esquema propio más allá del nombre del tópico.

## Cómputo verificado (ejemplo 5.A → 5.D)

Con `CLI-99821` (WHOLESALE, GENERAL, MX) y PRD-001/PRD-008 (STANDARD, ACTIVE, MX):

| Línea | qty | precio | gross | desc (3%) | neto | IVA 16% | total línea |
| --- | --- | --- | --- | --- | --- | --- | --- |
| PRD-001 | 24 | 35.50 | 852.00 | 25.56 | 826.44 | 132.23 | 958.67 |
| PRD-008 | 12 | 82.00 | 984.00 | 0.00 | 984.00 | 157.44 | 1 141.44 |
| **totales** | | | **1836.00** | **25.56** | **1810.44** | **289.67** | **2100.11** |

Bloqueado por los tests unitarios del `OrderCalculatorService` (HALF_UP, 2 decimales).

## Verificación manual pendiente (entorno local)

- `docker compose up --profile full` y envío del JSON 5.A vía `kcat`/UI de Kafdrop → verificar
  5.D y estado `APPROVED` en Mongo Express (`orders_db.orders`).
- Simular 404 de cliente (GET a productos inexistentes), 5xx (derribar `products-api`) y ver DLT.