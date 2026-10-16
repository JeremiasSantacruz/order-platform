# Estrategia de Equipo, CI/CD y Definition of Done

> Documento de liderazgo técnico: cómo se organiza un equipo de 4 para construir y mantener esta
> plataforma, cómo se hace CI/PR y cuándo una historia se considera "done".

## 1. Equipo de trabajo (4 personas)

| Rol | Enfoque | Artefactos que cuida |
| --- | --- | --- |
| **Tech lead (orden)** | Arquitectura, decisiones (ADRs), estándares, integración entre servicios | Proposal, ADR-001/002/003, repositorio raíz, docker-compose |
| **Ingeniero A — worker** | `order-processor` (Java/Spring): hexagonales, listener, repositorio, publisher | `order-processor/**`, tests unitarios, IT Testcontainers |
| **Ingeniero B — catálogo** | `products-api` (Go) + `clients-api` (NestJS) y sus contratos | Módulos de las APIs, seeds, tests de contrato |
| **Ingeniero C — infra/plataforma** | Docker Compose, CI/CD, entornos, monitoreo (Kafdrop, Mongo Express), observabilidad | `docker-compose.yml`, GitHub Actions, scripts de despliegue |

Los contratos (5.A–5.D) **no son una frontera compartida**: cada uno tiene un único
**propietario** (el productor del recurso) y se rige por la gobernanza descrita en la **§5**
(propiedad, versionado, validación en CI, coordinación entre equipos y dominio del proveedor).

## 2. Flujo de trabajo Git

- **Trunk-based con ramas cortas:** una rama por historia (máx. 1-2 días de vida). Ramas de
  feature: `feat/<worker|products|clients>/<historia>`, `fix/...`, `docs/...`.
- **Rebase antes de merge** (evitar merge-commits ruidosos); merge a `main` por **Pull Request**
  con al menos 1 aprobación de un revisor **distinto al autor** y de rol complementario cuando el
  cambio cruza contratos (ej. worker ↔ products-api).
- **Protección de `main`:** CI es condición de merge; prohibido push directo.
- **Convención de commits** (Conventional Commits): `feat:`, `fix:`, `refactor:`, `docs:`,
  `chore:` — con número de historia en el cuerpo.

## 3. Estrategia de CI/CD (GitHub Actions)

Pipeline por PR y por merge a `main`:

```yaml
# Pipeline resumido (equivalente en GitHub Actions)
jobs:
  order-processor:
    - cd order-processor && sh gradlew test        # 107 tests (unit + IT)
    - ./gradlew clean build                        # compila fat jar
  clients-api:
    - cd clients-api && npm ci
    - npm run lint && npm run build
    - npm test && npm run test:e2e                 # unit + e2e (10 + 4)
  products-api:
    - cd products-api && go build ./...
    - go vet ./... && go test ./...
  contracts:
    - pact verification: order-processor (consumidor) vs clients-api/products-api (5.B/5.C)
    - shape de respuestas contra 5.B/5.C y campos prohibidos (ver §5.5)
  docker-smoke:
    - docker compose up --profile full --build      # smoke end-to-end (5.A → 5.D)
    - verificación de un fallo no recuperable → mensaje en la DLT orders.processing.dlt con headers
```

**Gates del merge:** los 4 primeros jobs verdes + smoke opcional marcado *required* en `main` +
revisión humana. **CD:** tag `v*` → build de imágenes → push a registry → rollout por entorno
(staging primero; producción con feature-flags de mercado cuando aplique).

## 4. Entornos y configuración

- **Local sin credenciales:** seeds en memoria / infraestructura por defecto — los 3 servicios
  compilan y corren sin secretos externos (checklist §9).
- **Docker Compose:** infra (Kafka/Mongo/UIs) sin perfil; servicios con perfil `full`
  (`--profile full`).
- **Config externa** vía `env` (nunca hardcode): `KAFKA_BOOTSTRAP_SERVERS`, `MONGO_URI`,
  `CLIENTS_API_BASE_URL`, `PRODUCTS_API_BASE_URL`.
- **Observabilidad mínima:** logs estructurados SLF4J en el worker; Kafdrop para inspeccionar
  tópicos; Mongo Express para inspeccionar `orders`; healthchecks en ambas APIs; dashboards de
  Grafana para la traza por evento (`order-event-logs`) y para métricas de éxitos y DLT derivadas
  de los logs con LogQL (`order-processing-metrics`).

## 5. Gobernanza de contratos

### 5.1 Propiedad de cada contrato

**Regla general:** el **productor** (proveedor del recurso) es el dueño absoluto del contrato que
expone. Nadie más puede modificarlo unilateralmente; los consumidores participan únicamente a
través de sus expectativas de contrato (§5.3).

| Contrato | Propietario | Consumidor(es) |
| --- | --- | --- |
| Tópico `orders.created.v1` (5.A) | Sistema que origina los pedidos (orquestador que publica hacia Kafka) | `order-processor` como consumidor pasivo |
| `GET /clients/{clientId}` (5.B) | **clients-api** (NestJS) | `order-processor` |
| `GET /products/{productId}` (5.C) | **products-api** (Go) | `order-processor` |
| Tópico `orders.processed.v1` (5.D) | **order-processor** (productor del evento de resultado) | Consumidores de salida (facturación, notificaciones) |

### 5.2 Versionado de cambios incompatibles

- **REST:** versión explícita en la ruta — `/api/v1/...` vs `/api/v2/...` — reservada para
  *breaking changes*. Agregar un campo opcional no es breaking y va en la versión vigente.
- **Kafka:** versión en el nombre del tópico (`orders.created.v1`, `orders.processed.v1`) más el
  campo `eventVersion` en el payload como doble señal.
- **Principio de tolerancia (lado consumidor):** los adaptadores consumen ignorando campos nuevos
  desconocidos que envíe el productor. Si el cambio es crítico, se levanta una versión `v2` en
  paralelo, se da una ventana para que los consumidores migren y recién entonces se depreca `v1`.

### 5.3 Validación de contratos en CI (Consumer-Driven Contracts, Pact)

- `order-processor` genera los **pactos**: archivos de contrato que declaran qué espera recibir de
  `clients-api` y `products-api` (5.B/5.C) — estado, headers, shape y reglas de negocio mínimas.
- Los pipelines de CI de `products-api` (Go) y `clients-api` (NestJS) ejecutan la **verificación
  de pactos** antes de permitir el despliegue. Si un cambio del productor rompe lo que el
  consumidor necesita, **el pipeline falla automáticamente** y el merge queda bloqueado.
- Los pactos viven versionados junto al consumidor; el productor solo verifica, nunca edita las
  expectativas de otro equipo.

### 5.4 Coordinación cuando productor y consumidor son equipos distintos

1. **Contract-first:** el esquema OpenAPI o la definición del evento (5.A–5.D) es la fuente de
   verdad y se actualiza **primero**, en un PR revisado por ambos lados. Nada de cambios
   unilaterales del productor.
2. **Fase de expansión:** el productor implementa el campo o estructura nueva **sin eliminar** la
   anterior (coexisten temporalmente).
3. **Fase de migración:** se notifica al equipo consumidor para que actualice su adaptador y
   consuma la nueva forma; su pipeline de pactos valida la migración.
4. **Fase de contracción:** cuando todos los consumidores migraron, el productor elimina
   formalmente el atributo o versión antigua (aquí sí es breaking → nueva versión, §5.2).

### 5.5 Dominio del proveedor: qué se expone y qué no

- **Se expone** (solo lo esencial para que el consumidor cumpla su proceso):
  - Products API (5.C): `productId`, `name`, `status`, `taxCategory` — necesarios para calcular
    impuestos y validar elegibilidad.
  - Clients API (5.B): `clientId`, `status`, `segment`, `taxRegime`, `market`.
- **No se expone** (dominio interno del proveedor):
  - Credenciales o hashes de contraseñas, campos de autenticación.
  - Estructuras internas de persistencia (`__v` de MongoDB, ids internos de colecciones,
    esquemas de tablas PostgreSQL).
  - Metadatos de auditoría interna, timestamps de infraestructura, lógica de negocio propietaria
    del servicio que al consumidor no le incumbe.
- **Verificación:** los tests de contrato fallan si aparece un campo fuera de la spec (p. ej.
  `handler_test.go` en products-api rechaza campos prohibidos), de modo que filtrar estado
  interno es un fallo de CI, no una revisión manual.

## 6. Definition of Done (DoD)

Una historia se considera **done** cuando cumple **todas**:

1. Código implementado según los contratos 5.A–5.D vigentes (sin drift en wire types).
2. `domain/` y `application/` del worker sin import Spring/Kafka/Mongo (regla arquitectónica
   verificada).
3. Tests automatizados verdes: unit de cálculo con `BigDecimal`/`HALF_UP`, IT de integración con
   Testcontainers (o skip documentado), e2e API cuando el cambio toca HTTP.
4. Coherencia de consistencia: escenarios 7.1/7.2/7.3 cubiertos con prueba que demuestre el
   comportamiento (idempotencia, CAS, estados finales).
5. Si el cambio toca un contrato: actualizados spec + consumidores en el **mismo PR**.
6. Docker Compose levanta el conjunto (smoke 5.A → 5.D) y un fallo no recuperable termina publicado
   en la DLT `orders.processing.dlt` con sus headers; los rechazos de negocio siguen como
   `REJECTED` persistido y publicado en `orders.processed.v1`.
7. Deuda técnica nueva agregada a `implementation-notes.md` con trigger de cierre explícito.
8. ADR actualizada si la decisión arquitectónica cambia; de lo contrario, referencia a las
   existentes.
9. Documentación (README) al día con comandos de ejecución del cambio.
10. Revisión: PR aprobado por ≥ 1 revisor externo al autor.

## 7. Riesgos y plan de mitigación

| Riesgo | Probabilidad | Impacto | Mitigación |
| --- | --- | --- | --- |
| Drift de contrato entre servicios | Media | Alto | Propiedad explícita por contrato (§5.1); pactos Pact en CI (§5.3); expansión/contracción para cambios (§5.4); revisión cruzada del PR |
| Pérdida/perdida-ventana de eventos | Baja (por diseño) | Medio | ADR-002 B+E; re-entrega idempotente; estados finales persistidos; revisar outbox si cambian requisitos |
| Volumen/contención en Mongo | Baja | Medio | Índice `{eventId:1}` pendiente; CAS por `orderId`; evaluar partición por mercado |
| Kafka no disponible prolongado | Media | Medio | ACK por registro; retención del tópico de entrada y re-entrega idempotente; alerta operativa |
| Deuda de tests en catálogo Go | Media | Bajo | Pruebas de handler/usecase en siguiente sprint |

## 8. Métricas de salud del equipo

- **Ciclo de PR:** objetivo < 1 día laboral de ida y vuelta.
- **Coverage clave:** cálculo financiero 100% de ramas (unit); escenarios de concurrencia en IT.
- **Deuda técnica:** revisión quincenal del backlog de `implementation-notes.md`.
- **Retrospectiva/sprint:** clásica cada 2 semanas con foco en gobernanza de contratos (§5).