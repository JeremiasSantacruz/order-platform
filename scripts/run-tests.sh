#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<EOF
Uso: $(basename "$0") [opciones] [servicio...]

Servicios: order-processor | clients-api | products-api
Si no se indica ninguno, se ejecutan todos.

Opciones:
  -l, --lint     Ejecuta tambien lint/typecheck del servicio
  -h, --help     Muestra esta ayuda
EOF
}

RUN_LINT=0
SERVICES=()

for arg in "$@"; do
  case "$arg" in
    -l|--lint) RUN_LINT=1 ;;
    -h|--help) usage; exit 0 ;;
    order-processor|clients-api|products-api) SERVICES+=("$arg") ;;
    *) echo "Argumento desconocido: $arg" >&2; usage >&2; exit 1 ;;
  esac
done

if [ ${#SERVICES[@]} -eq 0 ]; then
  SERVICES=(order-processor clients-api products-api)
fi

FAILED=()

run_step() {
  echo ""
  echo "==> $1"
  shift
  if "$@"; then
    echo "==> OK: $*"
  else
    echo "==> FALLO: $*" >&2
    FAILED+=("$*")
  fi
}

for svc in "${SERVICES[@]}"; do
  case "$svc" in
    order-processor)
      if [ "$RUN_LINT" -eq 1 ]; then
        run_step "order-processor: compilando..." bash -c 'cd "$0"/order-processor && sh gradlew compileJava compileTestJava' "$ROOT_DIR"
      fi
      run_step "order-processor: tests" bash -c 'cd "$0"/order-processor && sh gradlew test' "$ROOT_DIR"
      ;;
    clients-api)
      if [ "$RUN_LINT" -eq 1 ]; then
        run_step "clients-api: lint" bash -c 'cd "$0"/clients-api && npm run lint' "$ROOT_DIR"
        run_step "clients-api: build (typecheck)" bash -c 'cd "$0"/clients-api && npm run build' "$ROOT_DIR"
      fi
      run_step "clients-api: unit tests" bash -c 'cd "$0"/clients-api && npm test' "$ROOT_DIR"
      run_step "clients-api: e2e tests" bash -c 'cd "$0"/clients-api && npm run test:e2e' "$ROOT_DIR"
      ;;
    products-api)
      if [ "$RUN_LINT" -eq 1 ]; then
        run_step "products-api: vet" bash -c 'cd "$0"/products-api && go vet ./...' "$ROOT_DIR"
      fi
      run_step "products-api: tests" bash -c 'cd "$0"/products-api && go test ./...' "$ROOT_DIR"
      ;;
  esac
done

echo ""
if [ ${#FAILED[@]} -gt 0 ]; then
  echo "FALLARON ${#FAILED[@]} paso(s):" >&2
  printf '  - %s\n' "${FAILED[@]}" >&2
  exit 1
fi
echo "Todos los tests pasaron."
