# Especificación Técnica: Plataforma Confiable de Pedidos B2B

---

## 1. Propósito y Criterios de Evaluación

Esta prueba evalúa la capacidad de un **Tech Lead** para diseñar, implementar y conducir técnicamente una solución distribuida en **Java**, **Go** y **NestJS**. La solución requiere reglas de negocio complejas, contratos entre servicios, procesamiento asíncrono y persistencia.

### Competencias Clave a Demonstrar

* **Arquitectura:** Separación estricta de dominio, aplicación e infraestructura.
* **Contratos:** Diseño, evolución y compatibilidad entre servicios.
* **Resiliencia y Consistencia:** Manejo explícito de concurrencia, duplicados, idempotencia y fallos parciales.
* **Calidad de Código:** Implementación acotada, mantenible, coherente y suite de pruebas sólida.
* **Liderazgo Técnico:** Definición de límites/ownership, estrategia de entrega incremental, estándares sin sobrediseño y resolución de incidentes.

---

## 2. Resumen Ejecutivo de la Prueba

| Parámetro                    | Detalle                                              |
|------------------------------|------------------------------------------------------|
| **Tiempo Recomendado**       | 12 horas (administradas por el candidato)            |
| **Tiempo Máximo de Entrega** | 3 días calendario                                    |
| **Worker Principal**         | Java 21+ con Spring Boot 3.x                         |
| **Products API**             | Go 1.22+                                             |
| **Clients API**              | NestJS con TypeScript                                |
| **Frontend Opcional**        | Flutter                                              |
| **Modalidad de Entrega**     | Repositorio Git y defensa técnica oral de 75 minutos |

> **Nota:** Se prioriza una solución pequeña, correcta e impecable por sobre un alcance incompleto u opcional.

---

## 3. Arquitectura del Sistema

```text
                ┌─────────────────────────┐
                │    orders.created.v1    │
                └───────────┬─────────────┘
                            │ (Kafka Consumer)
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    order-processor (Java)                    │
│                                                             │
│   ┌─────────────────┐ ┌─────────────────┐ ┌──────────────┐   │
│   │ Clients Client  │ │ Products Client │ │ Domain Rules │   │
│   └────────┬────────┘ └────────┬────────┘ └──────┬───────┘   │
└────────────┼───────────────────┼─────────────────┼───────────┘
             │ (HTTP)            │ (HTTP)          │
             ▼                   ▼                 │ (Persistence)
    ┌────────────────┐  ┌────────────────┐         ▼
    │  Clients API   │  │  Products API  │   ┌───────────┐
    │    (NestJS)    │  │      (Go)      │   │  MongoDB  │
    └────────────────┘  └────────────────┘   └───────────┘
                                                   │
                                                   ▼ (Publish Event)
                                        ┌─────────────────────────┐
                                        │   orders.processed.v1   │
                                        └─────────────────────────┘

```

---

## 4. Requisitos y Alcance Técnico

### Stack Tecnológico Obligatorio

* **Java 21+** & **Spring Boot 3.x** (`order-processor`)
* **Go 1.22+** (`products-api`)
* **NestJS & TypeScript** (`clients-api`)
* **Kafka** (Broker de eventos)
* **MongoDB** (Base de datos del worker)
* **Testcontainers** & **JUnit 5** (Pruebas de integración y unitarias)
* **Docker Compose** (Entorno local)

---

## 5. Especificación de Contratos

### A. Evento de Entrada (`orders.created.v1`)

* **Tópico Kafka:** `orders.created.v1`
* **Clave recomendada:** `orderId`

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
    {
      "productId": "PRD-001",
      "quantity": 24,
      "unitPrice": 35.5
    },
    {
      "productId": "PRD-008",
      "quantity": 12,
      "unitPrice": 82.0
    }
  ]
}

```

#### Reglas de Validación de Entrada:

1. `eventId`, `orderId`, `market`, `currency`, `clientId` y `items` son obligatorios.
2. `items` debe tener al menos 1 elemento.
3. No se permiten `productId` duplicados en el mismo pedido.
4. `quantity` > 0 (entero).
5. `unitPrice` >= 0.
6. Mercados válidos: `MX`, `CO`, `PE`.
7. Moneda debe corresponder al mercado: `MXN` (MX), `COP` (CO), `PEN` (PE).

---

### B. Clients API (`NestJS`)

* **Endpoint:** `GET /clients/{clientId}`
* **Respuesta 200 OK:**

```json
{
  "clientId": "CLI-99821",
  "name": "Distribuidora Central",
  "status": "ACTIVE",
  "segment": "WHOLESALE",
  "taxRegime": "GENERAL",
  "market": "MX"
}

```

* **Dominios de Enum:**
* `status`: `ACTIVE`, `BLOCKED`
* `segment`: `WHOLESALE`, `RETAIL`
* `taxRegime`: `GENERAL`, `SIMPLIFIED`, `EXEMPT`



---

### C. Products API (`Go`)

* **Endpoint:** `GET /products/{productId}?market={market}`
* **Respuesta 200 OK:**

```json
{
  "productId": "PRD-001",
  "name": "Bebida 600 ml",
  "sku": "BEB-600-PET",
  "status": "ACTIVE",
  "taxCategory": "STANDARD"
}

```

* **Dominios de Enum:**
* `status`: `ACTIVE`, `DISCONTINUED`
* `taxCategory`: `STANDARD`, `REDUCED`, `EXEMPT`



---

### D. Evento de Salida (`orders.processed.v1`)

* **Tópico Kafka:** `orders.processed.v1`

```json
{
  "eventId": "01J8ZP6M5E4RH0K7Y2N9A3TQWX-OUT",
  "eventVersion": 1,
  "occurredAt": "2026-09-18T15:42:11Z",
  "sourceEventId": "01J8ZP6M5E4RH0K7Y2N9A3TQWX",
  "orderId": "ORD-MX-000147",
  "status": "APPROVED",
  "market": "MX",
  "currency": "MXN",
  "totals": {
    "grossSubtotal": 1836.00,
    "discount": 25.56,
    "netSubtotal": 1810.44,
    "tax": 289.67,
    "grandTotal": 2100.11
  },
  "reason": null
}

```

---

## 6. Reglas de Negocio

### 1. Elegibilidad del Pedido

Un pedido solo se marca como `APPROVED` si cumple **todas** las siguientes condiciones:

* El cliente existe y su `status` es `ACTIVE`.
* El `market` del cliente coincide con el `market` del pedido.
* Todos los productos existen y tienen `status = ACTIVE`.

*(Si no cumple, el estado debe ser `REJECTED` con la razón explícita).*

---

### 2. Tabla de Impuestos por Mercado

| Mercado | STANDARD | REDUCED | EXEMPT |
|---------|----------|---------|--------|
| **MX**  | 16%      | 8%      | 0%     |
| **CO**  | 19%      | 5%      | 0%     |
| **PE**  | 18%      | 10%     | 0%     |

* **Excepción Fiscal:** Si el cliente tiene `taxRegime = EXEMPT`, la tasa efectiva es **0%** sin importar la categoría del producto.

---

### 3. Reglas de Descuento

* Si el cliente es `WHOLESALE` y compra **$\ge 20$ unidades** de una línea de producto, recibe **3% de descuento** sobre el `grossSubtotal` de esa línea.

---

### 4. Algoritmo Numérico y Redondeo

Por cada línea de producto:

1. $\text{grossSubtotal} = \text{quantity} \times \text{unitPrice}$
2. $\text{discount} = \text{grossSubtotal} \times \text{discountRate}$
3. $\text{netSubtotal} = \text{grossSubtotal} - \text{discount}$
4. $\text{taxAmount} = \text{netSubtotal} \times \text{taxRate}$
5. $\text{lineTotal} = \text{netSubtotal} + \text{taxAmount}$

> **Regla de Precisión:**
> * Utilizar exclusivamente **`BigDecimal`**.
> * Redondear cada importe monetario a **2 decimales** usando **`RoundingMode.HALF_UP`**.
> * Los totales del pedido resultan de sumar los importes ya redondeados de cada línea.
> 
> 

---

## 7. Idempotencia, Resiliencia y Manejo de Errores

### Matriz de Errores HTTP Internos

| Código HTTP     | Clasificación       | Acción del Worker                               |
|-----------------|---------------------|-------------------------------------------------|
| `404`           | Recurso Inexistente | Definitivo $\rightarrow$ Estado `REJECTED`      |
| `429`           | Rate Limit          | Transitorio $\rightarrow$ Reintento con Backoff |
| `500, 502, 503` | Error Interno       | Transitorio $\rightarrow$ Reintento con Backoff |
| Timeout         | Falla de Red        | Transitorio $\rightarrow$ Reintento con Backoff |

---

### Escenarios de Concurrencia a Manejar

1. **Re-entrega del mismo `eventId`:** No debe duplicar efectos ni cambiar el resultado persistido.
2. **Eventos concurrentes con mismo `orderId` y `eventVersion`:** Operación atómica de inserción/actualización con control de versión.
3. **Versión antigua recibida después de una versión más reciente:** Ignorar evento obsoleto.

---

### Resolución de Mensajes Fallidos (sin DLT)

No existe una DLT: **todo mensaje termina persistido en MongoDB y publicado en `orders.processed.v1`**
con su estado final. La clasificación se resuelve en una de dos capas:

* **En el listener / caso de uso (sin reintentos):**
  * Payload no parseable → pedido `REJECTED`, razón `DESERIALIZATION: <detalle>`; se conservan los
    identificadores legibles del payload (o de la clave del registro, o un id sintético
    `topic-partition-offset`) y el payload crudo en `sourcePayload`.
  * Violación del contrato (§5.A) → pedido `REJECTED`, razón `CONTRACT_VIOLATION: <reglas>`.
* **Al agotar los reintentos con backoff** (429/5xx/timeout o fallo interno repetido) → pedido
  `TECHNICAL_FAILURE`, razón `TECHNICAL_FAILURE: <causa raíz>`; si el payload ya no se puede leer,
  se registra como `REJECTED` no procesable.

La confirmación del offset se emite recién después de persistir y publicar (ack por registro):
si el procesamiento falla, Kafka redelivers el mensaje y la idempotencia por `eventId`/`eventVersion`
(§7) evita efectos duplicados.

---

## 8. Entregables y Estructura de Documentación

### Estructura de Repositorio Esperada

```text
.
├── order-processor/          # Servicio Java / Spring Boot
├── products-api/             # Servicio Go
├── clients-api/              # Servicio NestJS
├── docker-compose.yml        # Infraestructura completa local
├── README.md                 # Guía de ejecución, comandos y uso de IA
└── docs/
    ├── architecture-proposal.md  # Arquitectura detallada previo a codificar
    ├── implementation-notes.md   # Notas post-implementación y deuda técnica
    ├── technical-leadership.md   # Estrategia de equipo, CI/CD y Definition of Done
├── adr-001-concurrency.md    # ADR: Concurrencia y procesamiento
    ├── adr-002-consistency.md    # ADR: Consistencia Mongo - Kafka
    └── adr-003-failure-handling.md # ADR: Manejo de errores sin DLT
```

---

## 9. Checklists de Aprobación

### Checklist de Código

* [x] Los 3 microservicios compilan y ejecutan sin credenciales externas.
* [x] Dominio en Java libre de dependencias a Spring/Mongo/Kafka.
* [x] Tests unitarios del cálculo financiero con `BigDecimal` y `HALF_UP`.
* [x] Pruebas de integración con Testcontainers.
* [x] Docker Compose levanta todos los componentes.

### Checklist de Liderazgo Técnico (Documentación)

* [x] `architecture-proposal.md` con flujos principales y alternativos.
* [x] `technical-leadership.md` con plan para equipo de 4 personas y estrategia de CI/PRs.
* [x] Mencionadas herramientas de IA utilizadas y sugerencias descartadas en `README.md`.