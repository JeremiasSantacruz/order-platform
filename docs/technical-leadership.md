# Estrategia de Equipo, CI/CD y Definition of Done

> Documento de liderazgo técnico: cómo se organiza un equipo de 4 para construir y mantener esta
> plataforma, cómo se hace CI/PR y cuándo una historia se considera "done".

## 1. Equipo de trabajo (4 personas)

| Rol | Enfoque | Artefactos que cuida |
| --- | --- | --- |
| **Tech lead (orden)** | Arquitectura, decisiones (ADRs), estándares, integración entre servicios | Proposal, ADR-001/002, repositorio raíz, docker-compose |
| **Ingeniero A — worker** | `order-processor` (Java/Spring): hexagonales, listener, repositorio, publisher | `order-processor/**`, tests unitarios, IT Testcontainers |
| **Ingeniero B — catálogo** | `products-api` (Go) + `clients-api` (NestJS) y sus contratos | Módulos de las APIs, seeds, tests de contrato |
| **Ingeniero C — infra/plataforma** | Docker Compose, CI/CD, entornos, monitoreo (Kafdrop, Mongo Express), observabilidad | `docker-compose.yml`, GitHub Actions, scripts de despliegue |

Las áreas de **contrato** (5.A–5.D) son una **frontera compartida** y se tratan como tal:
cualquier cambio en el wire type de una API es un PR que toca la spec y los consumidores, revisado
por el equipo dueño del otro lado.

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
    - cd order-processor && ./gradlew test        # 81 tests (unit + IT)
    - ./gradlew clean build                        # compila fat jar
  clients-api:
    - cd clients-api && npm ci
    - npm run lint && npm run build
    - npm test && npm run test:e2e                 # unit + e2e (10 + 4)
  products-api:
    - cd products-api && go build ./...
    - go vet ./... && go test ./...
  contracts:
    - verificación de shape de respuestas contra 5.B/5.C (mock/vcr o pruebas de contrato)
  docker-smoke:
    - docker compose up --profile full --build      # smoke end-to-end (5.A → 5.D)
    - verificación de DLT ante un 5xx del catálogo
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
  tópicos/DLT; Mongo Express para inspeccionar `orders`; healthchecks en ambas APIs.

## 5. Definition of Done (DoD)

Una historia se considera **done** cuando cumple **todas**:

1. Código implementado según los contratos 5.A–5.D vigentes (sin drift en wire types).
2. `domain/` y `application/` del worker sin import Spring/Kafka/Mongo (regla arquitectónica
   verificada).
3. Tests automatizados verdes: unit de cálculo con `BigDecimal`/`HALF_UP`, IT de integración con
   Testcontainers (o skip documentado), e2e API cuando el cambio toca HTTP.
4. Coherencia de consistencia: escenarios 7.1/7.2/7.3 cubiertos con prueba que demuestre el
   comportamiento (idempotencia, CAS, DLT).
5. Si el cambio toca un contrato: actualizados spec + consumidores en el **mismo PR**.
6. Docker Compose levanta el conjunto (smoke 5.A → 5.D) y la DLT responde a un `404/5xx`.
7. Deuda técnica nueva agregada a `implementation-notes.md` con trigger de cierre explícito.
8. ADR actualizada si la decisión arquitectónica cambia; de lo contrario, referencia a las
   existentes.
9. Documentación (README) al día con comandos de ejecución del cambio.
10. Revisión: PR aprobado por ≥ 1 revisor externo al autor.

## 6. Riesgos y plan de mitigación

| Riesgo | Probabilidad | Impacto | Mitigación |
| --- | --- | --- | --- |
| Drift de contrato entre servicios | Media | Alto | Frontera de contrato compartida; pruebas de shape; revisión cruzada del PR |
| Pérdida/perdida-ventana de eventos | Baja (por diseño) | Medio | ADR-002 B+E; re-entrega idempotente; DLT; revisar outbox si cambian requisitos |
| Volumen/contención en Mongo | Baja | Medio | Índice `{eventId:1}` pendiente; CAS por `orderId`; evaluar partición por mercado |
| Kafka no disponible prolongado | Media | Medio | ACK por registro; proveer retención y política DLT; alerta operativa |
| Deuda de tests en catálogo Go | Media | Bajo | Pruebas de handler/usecase en siguiente sprint |

## 7. Métricas de salud del equipo

- **Ciclo de PR:** objetivo < 1 día laboral de ida y vuelta.
- **Coverage clave:** cálculo financiero 100% de ramas (unit); escenarios de concurrencia en IT.
- **Deuda técnica:** revisión quincenal del backlog de `implementation-notes.md`.
- **Retrospectiva/sprint:** clásica cada 2 semanas con foco en fronteras de contrato.