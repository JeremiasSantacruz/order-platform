# order-platform — Plataforma Confiable de Pedidos B2B

Worker de procesamiento de pedidos B2B y servicios de catálogo, según `Expecificaciones.md`:

- **order-processor** — Java/Spring Boot (hexagonal). Consume `orders.created.v1`, valida,
  consulta clientes/productos, calcula con `BigDecimal`/`HALF_UP`, persiste en MongoDB y publica
  `orders.processed.v1`; maneja idempotencia (7.1), concurrencia por versión (7.2/7.3) y reintentos
  con DLT (7).
- **clients-api** — NestJS. `GET /clients/{clientId}` (contrato 5.B).
- **products-api** — Go. `GET /products/{productId}?market={market}` (contrato 5.C).
- **Infra** — Docker Compose: Kafka, Zookeeper, MongoDB, Kafdrop, Mongo Express.

## Estructura

```
order/
├── order-processor/          # Worker Java (Spring Boot)
├── products-api/             # Catálogo Go
├── clients-api/              # Clientes NestJS
├── docker-compose.yml        # Infra + perfil 'full' para los 3 servicios
├── README.md
└── docs/
    ├── architecture-proposal.md
    ├── implementation-notes.md
    ├── technical-leadership.md
    ├── adr-001-concurrency.md
    └── adr-002-consistency.md
```

## Requisitos

- JDK 21 (verificado: MSJDK 21.0.12.1)
- Node.js ≥ 20 (verificado: v24.21.0) y npm
- Go ≥ 1.22 (verificado: go1.27.0)
- Docker + Docker Compose (para infra y pruebas con Testcontainers)

## Ejecución local

### Opción A — Solo infraestructura (recomendada para desarrollar)

```bash
docker compose up -d              # Kafka:9092, Mongo:27017, Kafdrop:9000, MongoExpress:8081
```

Levantar los servicios en local:

```bash
# order-processor (PowerShell)
$env:JAVA_HOME="<ruta-al-jdk21>"; .\gradlew.bat bootRun

# clients-api
cd clients-api && npm install && npm run start:dev      # http://localhost:3000

# products-api
cd products-api && go run ./cmd/api                     # http://localhost:8082
```

### Opción B — Todo con Docker (stack completo)

```bash
docker compose --profile full up --build
```

El worker usa el broker interno (`kafka:29092`) y las URLs internas de los servicios; no expone
puerto HTTP.

## Verificación end-to-end con el ejemplo de la spec (5.A → 5.D)

### Automática (recomendada): `scripts/smoke-test.sh`

Levanta el stack y valida **6 escenarios** con PASS/FAIL:

```bash
bash scripts/smoke-test.sh             # build + todos los tests
bash scripts/smoke-test.sh --no-build  # reutiliza imágenes ya construidas
bash scripts/smoke-test.sh --skip-dlt  # omite la prueba DLT (apaga products-api)
```

1. Camino feliz 5.A→5.D (APPROVED, totals 1836.00 / 25.56 / 1810.44 / 289.67 / 2100.11)
2. Cliente bloqueado → `REJECTED` (CLIENT_INACTIVE)
3. Producto inexistente → `REJECTED` (PRODUCT_NOT_FOUND)
4. Fallo transitorio → reintentos con backoff → DLT (`RETRIES_EXHAUSTED` + 7 headers), sin evento de salida
5. Idempotencia (7.1): re-entrega del mismo `eventId` sin duplicar el documento
6. Versión vieja (7.3): un evento `eventVersion` menor tras una mayor se ignora

### Manual (con Kafdrop UI)

Publicar en `orders.created.v1` (clave `orderId`) con la UI de Kafdrop (`http://localhost:9000`):

```json
{
  "eventId": "01J8ZP6M5E4RH0K7Y2N9A3TQWX",
  "eventVersion": 1,
  "occurredAt": "2026-09-18T15:42:10Z",
  "orderId": "ORD-MX-000147",
  "market": "MX",
  "currency": "MXN",
  "clientId": "CLI-99821",
  "channel": "C1",
  "items": [
    { "productId": "PRD-001", "quantity": 24, "unitPrice": 35.5 },
    { "productId": "PRD-008", "quantity": 12, "unitPrice": 82.0 }
  ]
}
```

Resultado esperado en `orders.processed.v1` (5.D): `status=APPROVED`, `totals` =
`{grossSubtotal: 1836.00, discount: 25.56, netSubtotal: 1810.44, tax: 289.67, grandTotal: 2100.11}`.

Casos de fallo para probar el DLT (`orders.processing.dlt`, 7 headers):

- Cliente `CLI-0003` (BLOCKED) → `REJECTED` con razón.
- Producto `PRD-003` (DISCONTINUED en PE) → `REJECTED`.
- Detener `products-api` (5xx/timeout) → reintentos con backoff → DLT.

## Tests

```bash
# order-processor (81 tests; 5 IT de Mongo skipsracted si no hay Docker)
$env:JAVA_HOME="<ruta-al-jdk21>"; .\gradlew.bat test

# clients-api (10 unit + 4 e2e)
cd clients-api; npm install; npm test; npm run test:e2e

# products-api (build + vet + test)
cd products-api; go build ./...; go vet ./...; go test ./...
```

## Puertos y endpoints

| Servicio | Puerto | Endpoint |
| --- | --- | --- |
| clients-api | 3000 | `GET /health`, `GET /clients/{clientId}` |
| products-api | 8082 | `GET /health`, `GET /products/{productId}?market=` |
| Kafka (local) | 9092 | tópicos `orders.created.v1`, `orders.processed.v1`, `orders.processing.dlt` |
| Kafdrop | 9000 | UI de tópicos |
| Mongo (local/compuesto) | 27017 | `orders_db.orders` (worker), `catalog_db` (catálogo) |
| Mongo Express | 8081 | UI de Mongo |

## Uso de IA (sección 9 de la spec)

### Herramientas de IA utilizadas

- **OpenCode** (asistente de codificación con sesiones de trabajo y subagentes): implementó la
  capa de aplicación del worker sobre el dominio existente, las interfaces de salida (Mongo, Kafka,
  HTTP), la suite de tests, la alineación de contratos con `clients-api`/`products-api` y esta
  documentación.
- **Búsqueda web / consultas puntuales** para confirmar APIs específicas de librerías
  (spring-kafka, Spring Data Mongo, Testcontainers) y evitar suposiciones de versión.
- **Subagente de exploración** para localizar y auditar el código existente (dominio Java, APIs
  NestJS/Go) antes de escribir código nuevo.

### Sugerencias de IA evaluadas **y descartadas** (con motivo)

| Sugerencia | Por qué se descartó |
| --- | --- |
| **Outbox transaccional Mongo que publica a Kafka** | Correcta en escenarios multi-consumidor o de alta carga, pero agrega colección + publicador + limpieza sin beneficio para el alcance del spec (un consumidor, pedidos B2B, Kafka disponible). Se documentó como evolución en ADR-002 y queda en deuda técnica con trigger de reevaluación. |
| **Lock distribuido con Redis/Redlock para el pedido** | Aporta una dependencia infraestructural nueva para resolver algo que Mongo resuelve con CAS atómico por `_id`/`eventVersion` (ADR-001). Riesgo de bloqueos vencidos y supervivencia operativa sin beneficio. |
| **Event-sourcing (colección `order-events` + proyección)** | Correcta para trazabilidad completa, sobredimensionada: el spec exige un estado final por pedido. Se dejó como línea futura. |
| **Kafka exactly-once (`read_committed` + transact idempotente producer)** | No alcanza la consistencia Mongo-Kafka (unidades transaccionales separadas), sobre-promete y complica el consumidor; se adoptó persistir + publicar bloqueante + re-entrega idempotente (ADR-002). |
| **`FAIL_ON_UNKNOWN_PROPERTIES=true` estricto en la lectura de eventos** | Payloads de productores futuros con campos nuevos romperían el consumidor; se mantuvo tolerancia a propiedades desconocidas con validación estricta de los campos del contrato (quantity entero). |
| **Crear índices Mongo en el arranque de Spring** | Conectarse a Mongo al arrancar quebraría los tests de contexto sin infraestructura; se difirió a una creación explícita (deuda técnica). |
| **Reintentos vía *retry topics* de Kafka (`RetryTopicConfiguration`)** | Más grados de movimiento y tópicos auxiliares; `DefaultErrorHandler` con backoff exponencial + DLT cubre el spec con menos superficie. |

## Documentación clave

- `docs/architecture-proposal.md` — diseño previo (flujos principal y alternativos).
- `docs/adr-001-concurrency.md` / `docs/adr-002-consistency.md` — decisiones de concurrencia y
  consistencia Mongo-Kafka.
- `docs/implementation-notes.md` — notas post-implementación y deuda técnica.
- `docs/technical-leadership.md` — plan de equipo, CI/CD y DoD.