#!/usr/bin/env bash
# =============================================================================
# smoke-test.sh — Smoke end-to-end de la plataforma B2B (Expecificaciones.md)
#
# Levanta el stack con Docker Compose (perfil "full") y verifica:
#   1. Camino feliz 5.A → 5.D  (APPROVED con los totals exactos del spec)
#   2. Cliente bloqueado       → REJECTED  (CLIENT_INACTIVE)
#   3. Producto inexistente    → REJECTED  (PRODUCT_NOT_FOUND)
#   4. Fallo transitorio       → reintentos con backoff → DLT (RETRIES_EXHAUSTED,
#      con los 7 headers del §7), sin evento de salida emitido
#   5. Idempotencia (7.1)      → re-entrega: 1 solo documento, sin duplicar estado
#   6. Versión vieja (7.3)     → el evento obsoleto se ignora; gana la v2
#
# Uso:
#   bash scripts/smoke-test.sh            # build + todos los tests
#   bash scripts/smoke-test.sh --no-build # reutiliza imágenes ya construidas
#   bash scripts/smoke-test.sh --skip-dlt # omite la prueba 4 (apaga products-api)
#
# Requisitos: Docker + Docker Compose v2 funcionando, y la CLI "docker" disponible
# en un shell bash (Git Bash / WSL / Linux / macOS).
# =============================================================================
set -uo pipefail

# --- Rutas ------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker-compose.yml"

# --- Flags ------------------------------------------------------------------
FLAG_BUILD=1
FLAG_DLT=1
for arg in "$@"; do
  case "$arg" in
    --no-build) FLAG_BUILD=0 ;;
    --skip-dlt) FLAG_DLT=0 ;;
    -h|--help) grep '^#' "$0" | sed -n '3,18p'; exit 0 ;;
    *) echo "Opción desconocida: $arg"; exit 2 ;;
  esac
done

# --- Colores y helpers ------------------------------------------------------
RED=$'\033[31m'; GRN=$'\033[32m'; YLW=$'\033[33m'; RST=$'\033[0m'
log()  { printf '[%s] %s\n' "$(date +%T)" "$*"; }
info() { printf '[%s] %s%s%s\n' "$(date +%T)" "$YLW" "$*" "$RST"; }
ok()   { printf '   %s✔ %s%s\n' "$GRN" "$*" "$RST"; }
ko()   { printf '   %s✘ %s%s\n' "$RED" "$*" "$RST"; }

declare -a RESULTS=()

docker() { command docker "$@"; }

preflight() {
  command -v docker >/dev/null 2>&1 || { echo "docker no está instalado/en PATH"; exit 1; }
  docker info >/dev/null 2>&1 || { echo "El daemon de Docker no está corriendo. Inicia Docker Desktop."; exit 1; }
  docker compose version >/dev/null 2>&1 || { echo "Docker Compose v2 no disponible"; exit 1; }
}

compose() { docker compose --profile full -f "$COMPOSE_FILE" "$@"; }

# Espera a que un contenedor esté running y healthy (o sin healthcheck definido).
wait_healthy() {
  local name="$1" tries="${2:-90}"
  for ((i = 1; i <= tries; i++)); do
    local state health
    state="$(docker inspect -f '{{.State.Status}}' "$name" 2>/dev/null || echo 'missing')"
    health="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$name" 2>/dev/null || echo 'none')"
    if [ "$state" = "running" ] && { [ "$health" = "healthy" ] || [ "$health" = "none" ]; }; then
      return 0
    fi
    sleep 2
  done
  log "Timeout esperando a $name (state=$state health=$health)"
  return 1
}

# Publica un evento en un tópico: clave = orderId (partición por pedido).
produce() {
  local topic="$1" key="$2" value="$3"
  printf '%s\t%s\n' "$key" "$value" |
    compose exec -T kafka kafka-console-producer \
      --bootstrap-server kafka:29092 --topic "$topic" --property parse.key=true 2>/dev/null
}

# Consume mensajes de un tópico (grupo nuevo, desde el inicio).
consume() {
  local topic="$1" n="$2" tms="$3" group="smoke-$$-$RANDOM"
  compose exec -T kafka kafka-console-consumer \
    --bootstrap-server kafka:29092 --topic "$topic" --group "$group" \
    --from-beginning --max-messages "$n" --timeout-ms "$tms" \
    --property print.key=false 2>/dev/null || true
}

# Consume la DLT imprimiendo también los headers (para validar los 7 del §7).
consume_dlt() {
  local topic="$1" n="$2" tms="$3" group="smoke-dlt-$$-$RANDOM"
  compose exec -T kafka kafka-console-consumer \
    --bootstrap-server kafka:29092 --topic "$topic" --group "$group" \
    --from-beginning --max-messages "$n" --timeout-ms "$tms" \
    --property print.headers=true --property print.key=false 2>/dev/null || true
}

# Estado del pedido desde Mongo (fuente de verdad): JSON con los campos clave.
mongo_doc() {
  local oid="$1"
  compose exec -T -e "OID=$oid" db-product mongosh \
    'mongodb://product:secret@localhost:27017/orders_db?authSource=admin' --quiet --eval '
      const d = db.orders.findOne({orderId: process.env.OID});
      if (!d) { print(JSON.stringify({found: false})); quit(); }
      const t = d.totals || {};
      const num = (b) => (b && typeof b.toString === "function") ? b.toString() : (b === null || b === undefined ? null : String(b));
      print(JSON.stringify({
        found: true,
        status: d.status,
        eventVersion: d.eventVersion,
        gross: num(t.grossSubtotal),
        discount: num(t.discount),
        net: num(t.netSubtotal),
        tax: num(t.tax),
        grand: num(t.grandTotal),
        reason: d.rejectionReason,
        count: db.orders.countDocuments({orderId: process.env.OID})
      }));
    ' 2>/dev/null
}

# Sondea Mongo hasta que el pedido alcance el status esperado (timeout en segundos).
wait_status() {
  local oid="$1" expected="$2" tries="$(( $3 / 2 ))" i doc status
  for ((i = 1; i <= tries; i++)); do
    doc="$(mongo_doc "$oid")"
    status="$(printf '%s' "$doc" | grep -o '"status":"[^"]*"' | head -1 | cut -d'"' -f4)"
    [ "$status" = "$expected" ] && return 0
    sleep 2
  done
  return 1
}

assert_in() { # assert_in <texto> <substring> <etiqueta>
  case "$1" in *"$2"*) ok "$3"; return 0 ;; *) ko "$3 (no se encontró: $2)" ;; esac
  return 1
}

run() { # run <nombre> <fn...>
  local name="$1"; shift
  log "$name"
  if "$@"; then RESULTS+=("PASS|$name"); else RESULTS+=("FAIL|$name"); fi
}

# --- Construcción del stack -------------------------------------------------
preflight

if [ "$FLAG_BUILD" = "1" ]; then
  log "Construyendo y levantando el stack completo (perfil full)…"
  compose up -d --build
else
  log "Levantando el stack completo (perfil full, sin rebuild)…"
  compose up -d
fi

log "Esperando a que los servicios estén listos…"
wait_healthy order-mongo        || exit 1
wait_healthy order-kafka        || exit 1
wait_healthy order-clients-api  || exit 1
wait_healthy order-products-api || exit 1
wait_healthy order-processor 20 || exit 1
ok "stack listo"

RUN="$(date +%s)"

# ============================================================================
# 1) Camino feliz: ejemplo 5.A → 5.D (mismos datos que la spec) → APPROVED
# ============================================================================
test_happy() {
  local oid="ORD-MX-000147-run$RUN" eid="EVT-HAPPY-run$RUN"
  local payload='{"eventId":"'"$eid"'","eventVersion":1,"occurredAt":"2026-09-18T15:42:10Z","orderId":"'"$oid"'","market":"MX","currency":"MXN","clientId":"CLI-99821","channel":"C1","items":[{"productId":"PRD-001","quantity":24,"unitPrice":35.5},{"productId":"PRD-008","quantity":12,"unitPrice":82.0}]}'
  info "publicando $oid (eventId=$eid)"
  produce orders.created.v1 "$oid" "$payload"

  wait_status "$oid" APPROVED 40 || { ko "APPROVED tras 40 s"; return 1; }

  local doc; doc="$(mongo_doc "$oid")"
  local okc=1
  assert_in "$doc" '"status":"APPROVED"'   "status=APPROVED"   || okc=0
  assert_in "$doc" '"gross":"1836.00"'     "gross 1836.00"     || okc=0
  assert_in "$doc" '"discount":"25.56"'    "discount 25.56"    || okc=0
  assert_in "$doc" '"net":"1810.44"'       "net 1810.44"       || okc=0
  assert_in "$doc" '"tax":"289.67"'        "tax 289.67"        || okc=0
  assert_in "$doc" '"grand":"2100.11"'     "grand 2100.11"     || okc=0
  assert_in "$doc" '"count":1'             "1 solo documento"  || okc=0

  # El evento de salida emitido debe reproducir los mismos totals (JSON compacto).
  local out; out="$(consume orders.processed.v1 2000 10000)"
  assert_in "$out" '"orderId":"'"$oid"'"'  "orden presente en orders.processed.v1" || okc=0
  local line; line="$(printf '%s' "$out" | grep -F "\"orderId\":\"$oid\"" | head -1)"
  assert_in "$line" '"grossSubtotal":1836.00' "5.D grossSubtotal=1836.00" || okc=0
  assert_in "$line" '"grandTotal":2100.11'    "5.D grandTotal=2100.11"   || okc=0
  assert_in "$line" '"status":"APPROVED"'     "5.D status=APPROVED"      || okc=0

  return "$okc"
}

# ============================================================================
# 2) Cliente bloqueado → REJECTED (CLIENT_INACTIVE)
# ============================================================================
test_blocked() {
  local oid="ORD-SMK-BLOCKED-run$RUN" eid="EVT-BLOCKED-run$RUN"
  # CLI-0003 es MX + BLOCKED: el market coincide, la elegibilidad falla por estado.
  local payload='{"eventId":"'"$eid"'","eventVersion":1,"occurredAt":"2026-09-18T15:42:10Z","orderId":"'"$oid"'","market":"MX","currency":"MXN","clientId":"CLI-0003","channel":"C1","items":[{"productId":"PRD-001","quantity":1,"unitPrice":10}]}'
  produce orders.created.v1 "$oid" "$payload"

  wait_status "$oid" REJECTED 40 || { ko "REJECTED tras 40 s"; return 1; }
  local doc; doc="$(mongo_doc "$oid")"
  local okc=1
  assert_in "$doc" '"status":"REJECTED"' "status=REJECTED"         || okc=0
  assert_in "$doc" 'CLIENT_INACTIVE'     "razón CLIENT_INACTIVE"    || okc=0
  return "$okc"
}

# ============================================================================
# 3) Producto inexistente → REJECTED (PRODUCT_NOT_FOUND)
# ============================================================================
test_not_found() {
  local oid="ORD-SMK-PNF-run$RUN" eid="EVT-PNF-run$RUN"
  local payload='{"eventId":"'"$eid"'","eventVersion":1,"occurredAt":"2026-09-18T15:42:10Z","orderId":"'"$oid"'","market":"MX","currency":"MXN","clientId":"CLI-99821","channel":"C1","items":[{"productId":"PRD-99999","quantity":1,"unitPrice":10}]}'
  produce orders.created.v1 "$oid" "$payload"

  wait_status "$oid" REJECTED 40 || { ko "REJECTED tras 40 s"; return 1; }
  local doc; doc="$(mongo_doc "$oid")"
  local okc=1
  assert_in "$doc" '"status":"REJECTED"' "status=REJECTED"       || okc=0
  assert_in "$doc" 'PRODUCT_NOT_FOUND'   "razón PRODUCT_NOT_FOUND" || okc=0
  return "$okc"
}

# ============================================================================
# 4) Fallo transitorio → reintentos con backoff → DLT (RETRIES_EXHAUSTED)
#    (apaga products-api temporalmente; sin evento de salida emitido)
# ============================================================================
test_dlt() {
  local oid="ORD-SMK-DLT-run$RUN" eid="EVT-DLT-run$RUN"
  local payload='{"eventId":"'"$eid"'","eventVersion":1,"occurredAt":"2026-09-18T15:42:10Z","orderId":"'"$oid"'","market":"MX","currency":"MXN","clientId":"CLI-99821","channel":"C1","items":[{"productId":"PRD-001","quantity":1,"unitPrice":10}]}'

  log "apagando products-api para inducir el fallo…"
  compose stop products-api
  sleep 2
  produce orders.created.v1 "$oid" "$payload"
  info "esperando reintentos + backoff (≈12-15 s)…"
  sleep 20

  local dlt_out; dlt_out="$(consume_dlt orders.processing.dlt 50 10000)"
  local okc=1
  assert_in "$dlt_out" "\"orderId\":\"$oid\"" "mensaje llegó a la DLT" || okc=0
  # El valor DLT es el evento original; errorCategory y attemptCount viven en headers.
  local line; line="$(printf '%s' "$dlt_out" | grep -F "\"orderId\":\"$oid\"" | head -1)"
  assert_in "$line" 'RETRIES_EXHAUSTED' "header errorCategory=RETRIES_EXHAUSTED" || okc=0
  assert_in "$line" 'errorCategory'     "header errorCategory presente"          || okc=0
  assert_in "$line" 'attemptCount'      "header attemptCount presente"           || okc=0
  assert_in "$line" 'summaryCause'      "header summaryCause presente"           || okc=0
  assert_in "$line" 'timestamp'         "header timestamp presente"              || okc=0
  assert_in "$line" 'component'         "header component presente"              || okc=0

  # Los fallos transitorios agotados NO deben producir evento de salida.
  local out; out="$(consume orders.processed.v1 2000 8000)"
  if printf '%s' "$out" | grep -qF "\"orderId\":\"$oid\""; then
    ko "no debe existir orders.processed.v1 para $oid"; okc=0
  else
    ok "sin evento de salida para un fallo agotado"
  fi

  log "restaurando products-api…"
  compose start products-api >/dev/null
  wait_healthy order-products-api || { ko "products-api no volvió a estar healthy"; return 1; }
  return "$okc"
}

# ============================================================================
# 5) Idempotencia (7.1): re-entrega del MISMO eventId ⇒ 1 solo documento
# ============================================================================
test_idempotent() {
  local oid="ORD-MX-000147-run$RUN" eid="EVT-HAPPY-run$RUN"
  local payload='{"eventId":"'"$eid"'","eventVersion":1,"occurredAt":"2026-09-18T15:42:10Z","orderId":"'"$oid"'","market":"MX","currency":"MXN","clientId":"CLI-99821","channel":"C1","items":[{"productId":"PRD-001","quantity":24,"unitPrice":35.5},{"productId":"PRD-008","quantity":12,"unitPrice":82.0}]}'
  info "repitiendo el evento 5.A del test 1 (mismos eventId/orderId)…"
  produce orders.created.v1 "$oid" "$payload"
  sleep 8

  local doc; doc="$(mongo_doc "$oid")"
  local okc=1
  assert_in "$doc" '"count":1'        "sigue habiendo 1 solo documento" || okc=0
  assert_in "$doc" '"grand":"2100.11"' "totals intactos (2100.11)"       || okc=0
  return "$okc"
}

# ============================================================================
# 6) Versión vieja tras una más reciente (7.3): el evento obsoleto se ignora
# ============================================================================
test_stale() {
  local oid="ORD-SMK-VER-run$RUN"
  local e1="EVT-VER-v1-run$RUN" e2="EVT-VER-v2-run$RUN" e3="EVT-VER-v1b-run$RUN"
  local v1='{"eventId":"'"$e1"'","eventVersion":1,"occurredAt":"2026-09-18T15:42:10Z","orderId":"'"$oid"'","market":"MX","currency":"MXN","clientId":"CLI-99821","channel":"C1","items":[{"productId":"PRD-001","quantity":1,"unitPrice":10}]}'
  local v2='{"eventId":"'"$e2"'","eventVersion":2,"occurredAt":"2026-09-18T15:43:10Z","orderId":"'"$oid"'","market":"MX","currency":"MXN","clientId":"CLI-99821","channel":"C1","items":[{"productId":"PRD-001","quantity":3,"unitPrice":10}]}'
  local v1b='{"eventId":"'"$e3"'","eventVersion":1,"occurredAt":"2026-09-18T15:44:10Z","orderId":"'"$oid"'","market":"MX","currency":"MXN","clientId":"CLI-99821","channel":"C1","items":[{"productId":"PRD-001","quantity":1,"unitPrice":10}]}'

  produce orders.created.v1 "$oid" "$v1"; sleep 4
  produce orders.created.v1 "$oid" "$v2"; sleep 4
  info "publicando versión v1 obsoleta (eventVersion=1) tras la v2…"
  produce orders.created.v1 "$oid" "$v1b"; sleep 6

  local doc; doc="$(mongo_doc "$oid")"
  local okc=1
  assert_in "$doc" '"eventVersion":2'  "eventVersion quedó en 2"   || okc=0
  assert_in "$doc" '"gross":"30.00"'   "totals de la v2 (30.00)"   || okc=0
  assert_in "$doc" '"count":1'         "1 solo documento"          || okc=0
  return "$okc"
}

# ============================================================================
# Ejecución y resumen
# ============================================================================
run "1) Camino feliz 5.A→5.D (APPROVED, totals exactos)"       test_happy
run "2) Cliente bloqueado → REJECTED (CLIENT_INACTIVE)"         test_blocked
run "3) Producto inexistente → REJECTED (PRODUCT_NOT_FOUND)"    test_not_found
if [ "$FLAG_DLT" = "1" ]; then
  run "4) Fallo transitorio → reintentos → DLT (RETRIES_EXHAUSTED)" test_dlt
else
  info "4) omitida (--skip-dlt)"
fi
run "5) Idempotencia (7.1): re-entrega sin duplicar estado"     test_idempotent
run "6) Versión vieja ignorada (7.3)"                           test_stale

printf '\n%-62s\n' "────────────────────────────────────────────────────────────────"
printf '  SMOKE TEST %s\n' "$(date)"
passed=0; failed=0
for r in "${RESULTS[@]}"; do
  IFS='|' read -r st name <<< "$r"
  if [ "$st" = "PASS" ]; then printf '  %s✔ %s%s\n' "$GRN" "$name" "$RST"; passed=$((passed+1));
  else                      printf '  %s✘ %s%s\n' "$RED" "$name" "$RST"; failed=$((failed+1)); fi
done
printf '%s%d%s OK, %s%d%s FAIL\n' "$GRN" "$passed" "$RST" "$RED" "$failed" "$RST"

[ "$failed" -eq 0 ]