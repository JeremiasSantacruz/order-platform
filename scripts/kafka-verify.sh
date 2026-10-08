#!/usr/bin/env bash
# =============================================================================
# kafka-verify.sh — Publica eventos de Kafka y verifica el resultado en la BD
#
# Envía mensajes a "orders.created.v1" con la herramienta Go de
# scripts/kafka-verify y valida:
#   1. happy       → APPROVED con los totales exactos (sección 5.A → 5.D)
#   2. blocked     → REJECTED / CLIENT_INACTIVE
#   3. notfound    → REJECTED / PRODUCT_NOT_FOUND
#   4. idempotent  → re-entrega del mismo eventId: 1 solo documento en Mongo
#
# Cada caso consulta MongoDB (orders_db.orders) como fuente de verdad y además
# confirma el evento de salida orders.processed.v1.
#
# Uso:
#   bash scripts/kafka-verify.sh                     # todos los casos
#   bash scripts/kafka-verify.sh -cases happy        # un solo caso
#   bash scripts/kafka-verify.sh -cases happy,blocked -timeout 60s
#
# Flags extra se pasan directo al binario Go (ver: go run . -h)
# Requisitos: stack levantado (docker compose up -d) y Go 1.22+.
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOOL_DIR="$SCRIPT_DIR/kafka-verify"

RED=$'\033[31m'; GRN=$'\033[32m'; YLW=$'\033[33m'; RST=$'\033[0m'
log() { printf '[%s] %s\n' "$(date +%T)" "$*"; }

command -v go >/dev/null 2>&1 || { echo "${RED}✘${RST} Go no está instalado/en PATH"; exit 1; }

# Preflight: Kafka y MongoDB accesibles desde el host.
for target in "9092 (Kafka)" "27017 (MongoDB)"; do
  port="${target%% *}"
  if ! (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
    echo "${RED}✘${RST} No responde 127.0.0.1:$port — ¿está el stack levantado? (docker compose up -d)"
    exit 1
  fi
done
log "${YLW}Kafka y MongoDB responden; ejecutando kafka-verify…${RST}"

cd "$TOOL_DIR"
exec go run . "$@"
