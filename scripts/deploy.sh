#!/usr/bin/env bash
set -euo pipefail

NS="${NS:-temporis}"
IMAGE_NAME="${IMAGE_NAME:-temporis:latest}"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "error: required command not found: $1" >&2
    exit 1
  fi
}

b64_encode() {
  printf '%s' "$1" | base64 | tr -d '\n'
}

b64_decode() {
  if base64 --decode >/dev/null 2>&1 <<<'dGVzdA=='; then
    base64 --decode
  else
    base64 -D
  fi
}

secret_value() {
  local key="$1"
  local encoded

  encoded="$(kubectl get secret timer-secret -n "${NS}" \
    -o "go-template={{ index .data \"${key}\" }}" 2>/dev/null || true)"
  if [ -z "${encoded}" ] || [ "${encoded}" = "<no value>" ]; then
    return 1
  fi

  printf '%s' "${encoded}" | b64_decode
}

patch_secret_key() {
  local key="$1"
  local value="$2"

  kubectl patch secret timer-secret -n "${NS}" --type merge \
    -p "{\"data\":{\"${key}\":\"$(b64_encode "${value}")\"}}" >/dev/null
}

database_password_from_url() {
  printf '%s' "$1" | sed -n 's#^postgres://postgres:\([^@]*\)@database:5432/temporis?sslmode=disable$#\1#p'
}

cache_password_from_url() {
  printf '%s' "$1" | sed -n 's#^redis://:\([^@]*\)@cache:6379$#\1#p'
}

ensure_timer_secret() {
  local database_password cache_password database_url cache_url

  if ! kubectl get secret timer-secret -n "${NS}" >/dev/null 2>&1; then
    database_password="$(openssl rand -hex 32)"
    cache_password="$(openssl rand -hex 32)"
    database_url="postgres://postgres:${database_password}@database:5432/temporis?sslmode=disable"
    cache_url="redis://:${cache_password}@cache:6379"

    kubectl create secret generic timer-secret -n "${NS}" \
      --from-literal=database-password="${database_password}" \
      --from-literal=cache-password="${cache_password}" \
      --from-literal=database-url="${database_url}" \
      --from-literal=cache-url="${cache_url}" >/dev/null
    echo "Created timer-secret."
    return
  fi

  database_password="$(secret_value database-password || true)"
  database_url="$(secret_value database-url || true)"
  if [ -z "${database_password}" ] && [ -n "${database_url}" ]; then
    database_password="$(database_password_from_url "${database_url}")"
  fi
  if [ -z "${database_password}" ]; then
    database_password="$(openssl rand -hex 32)"
  fi
  if [ -z "${database_url}" ]; then
    database_url="postgres://postgres:${database_password}@database:5432/temporis?sslmode=disable"
  fi
  if [ "${database_url}" != "postgres://postgres:${database_password}@database:5432/temporis?sslmode=disable" ]; then
    echo "error: timer-secret database-url does not match database-password; refusing to overwrite existing values" >&2
    exit 1
  fi

  cache_password="$(secret_value cache-password || true)"
  cache_url="$(secret_value cache-url || true)"
  if [ -z "${cache_password}" ] && [ -n "${cache_url}" ]; then
    cache_password="$(cache_password_from_url "${cache_url}")"
  fi
  if [ -z "${cache_password}" ]; then
    cache_password="$(openssl rand -hex 32)"
  fi
  if [ -z "${cache_url}" ]; then
    cache_url="redis://:${cache_password}@cache:6379"
  fi
  if [ "${cache_url}" != "redis://:${cache_password}@cache:6379" ]; then
    echo "error: timer-secret cache-url does not match cache-password; refusing to overwrite existing values" >&2
    exit 1
  fi

  secret_value database-password >/dev/null 2>&1 || patch_secret_key database-password "${database_password}"
  secret_value cache-password >/dev/null 2>&1 || patch_secret_key cache-password "${cache_password}"
  secret_value database-url >/dev/null 2>&1 || patch_secret_key database-url "${database_url}"
  secret_value cache-url >/dev/null 2>&1 || patch_secret_key cache-url "${cache_url}"
}

for cmd in kubectl docker make openssl; do
  require_cmd "${cmd}"
done

echo "Deploying Temporis..."
echo "Building image ${IMAGE_NAME}..."
make build
docker build -t "${IMAGE_NAME}" src

echo "Ensuring namespace ${NS} exists..."
kubectl create namespace "${NS}" --dry-run=client -o yaml | kubectl apply -f -

echo "Ensuring timer-secret exists..."
ensure_timer_secret

echo "Applying database init ConfigMap..."
kubectl create configmap database-init-script \
  --from-file=pkg/database/schema.sql \
  --dry-run=client \
  -o yaml \
  -n "${NS}" | kubectl apply -n "${NS}" -f -

echo "Applying Kubernetes manifests..."
kubectl apply -f deploy/ -n "${NS}"

echo "Waiting for database StatefulSet..."
kubectl rollout status statefulset/database -n "${NS}" --timeout=180s

echo "Restarting Temporis backend..."
kubectl rollout restart deployment/temporis -n "${NS}"
kubectl rollout status deployment/temporis -n "${NS}" --timeout=180s

echo "Deployment complete."
echo "Namespace: ${NS}"
echo "Context: $(kubectl config current-context)"
echo "Service: temporis.${NS}.svc.cluster.local:7946"
