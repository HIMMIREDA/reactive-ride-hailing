#!/usr/bin/env bash
# One-shot: registers the Avro schemas from common/src/main/resources/avro in Schema Registry,
# under the same subjects as the schema-registry-maven-plugin config in common/pom.xml.
# The services keep auto.register.schemas=false: producers can't publish a schema nobody registered.
# Idempotent: registering an identical schema returns its existing id. An incompatible change fails.
set -euo pipefail

REGISTRY="${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}"
SCHEMAS="${SCHEMAS_DIR:-/schemas}"

register() {
  local subject=$1 file=$2
  # Wrap the schema as a JSON string: {"schema": "<escaped schema>"} (the .avsc files contain no backslashes).
  local payload
  payload="{\"schema\":\"$(tr -d '\n\r' < "$SCHEMAS/$file" | sed 's/"/\\"/g')\"}"
  local response status
  # Status checked by hand: the curl in this image predates --fail-with-body.
  response=$(curl -sS -w '\n%{http_code}' -X POST \
    -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
    --data "$payload" "$REGISTRY/subjects/$subject/versions")
  status=${response##*$'\n'}
  response=${response%$'\n'*}
  if [ "$status" != 200 ]; then
    echo "failed to register $subject (HTTP $status): $response" >&2
    exit 1
  fi
  echo "registered $subject -> $response"
}

register poc-ride-request-requested-v1-value ride-request-requested-schema.avsc
register poc-ride-proposal-accepted-v1-value ride-proposal-accepted-schema.avsc
register poc-ride-proposal-booked-v1-value   ride-proposal-booked-schema.avsc
register poc-ride-proposal-proposed-v1-value ride-proposal-proposed-schema.avsc
