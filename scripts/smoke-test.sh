#!/usr/bin/env bash
# End-to-end smoke test against the running docker compose stack, through the API gateway only:
# sign up a rider and a driver, log in, create a ride, the driver sends an offer, the rider accepts,
# and the ride must end BOOKED. Exits non-zero on the first failure.
#
#   docker compose -f spring-webflux-poc/docker-compose.yaml up -d --build --wait
#   scripts/smoke-test.sh
#
# Requires curl, jq and docker. GATEWAY_URL defaults to http://localhost:8085.
# The final BOOKED check reads PostgreSQL through `docker compose exec`: the API has no endpoint that
# returns a ride's status, and BOOKED is set asynchronously (Kafka) after the accept call returns.
set -euo pipefail

GATEWAY_URL="${GATEWAY_URL:-http://localhost:8085}"
COMPOSE_FILE="${COMPOSE_FILE:-$(cd "$(dirname "$0")/.." && pwd)/spring-webflux-poc/docker-compose.yaml}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-60}"
RUN_ID="$(date +%s)-$RANDOM"

for tool in curl jq docker; do
  command -v "$tool" >/dev/null || { echo "missing required tool: $tool" >&2; exit 2; }
done

step() { printf '\n==> %s\n' "$*"; }
ok()   { printf '    ok: %s\n' "$*"; }
fail() { printf '    FAILED: %s\n' "$*" >&2; [ -n "${BODY:-}" ] && printf '    last response (HTTP %s): %s\n' "${STATUS:-?}" "$BODY" >&2; exit 1; }

# call METHOD PATH [TOKEN] [JSON_BODY] -> sets STATUS and BODY
call() {
  local method=$1 path=$2 token=${3:-} data=${4:-}
  local args=(-sS -o /tmp/smoke-body.$$ -w '%{http_code}' -X "$method" "$GATEWAY_URL$path" -H 'Content-Type: application/json')
  [ -n "$token" ] && args+=(-H "Authorization: Bearer $token")
  [ -n "$data" ] && args+=(--data "$data")
  STATUS=$(curl "${args[@]}") || STATUS=000
  BODY=$(cat /tmp/smoke-body.$$ 2>/dev/null || true)
  rm -f /tmp/smoke-body.$$
}

expect_status() { [ "$STATUS" = "$1" ] || fail "$2: expected HTTP $1, got $STATUS"; }

signup() { # email role first last
  call POST /api/users "" "$(jq -nc --arg e "$1" --arg r "$2" --arg f "$3" --arg l "$4" \
    '{email:$e, password:"smoke-pass-1234", firstName:$f, lastName:$l, role:$r, phoneNumber:"0600000000"}')"
  expect_status 201 "sign up $2"
  ok "$2 $1 created (id $(jq -r .id <<<"$BODY"))"
}

login() { # email role -> echoes token
  call POST /api/users/auth "" "$(jq -nc --arg e "$1" --arg r "$2" '{email:$e, password:"smoke-pass-1234", role:$r}')"
  expect_status 200 "log in $2"
  jq -er .token <<<"$BODY" || fail "no token in login response"
}

RIDER="rider-$RUN_ID@example.com"
DRIVER="driver-$RUN_ID@example.com"

step "Gateway health"
call GET /actuator/health
expect_status 200 "gateway health"
ok "$(jq -c . <<<"$BODY")"

step "Sign up a rider (USER) and a driver (DRIVER)"
signup "$RIDER" USER Ada Lovelace
signup "$DRIVER" DRIVER Grace Hopper

step "Log in"
RIDER_TOKEN=$(login "$RIDER" USER)
DRIVER_TOKEN=$(login "$DRIVER" DRIVER)
ok "two JWTs issued"

step "Rider creates a ride request"
call POST /api/rides "$RIDER_TOKEN" '{"startLng":20.2,"startLat":33.2,"endLng":20.1,"endLat":50.1,"seats":2,"preferredPrice":12.5}'
expect_status 200 "create ride"
RIDE_ID=$(jq -er .rideRequestId <<<"$BODY") || fail "no rideRequestId"
[ "$(jq -r .status <<<"$BODY")" = REQUESTED ] || fail "new ride is not REQUESTED"
ok "ride $RIDE_ID is REQUESTED"

step "Driver sends an offer (retried until driver-service has received the ride over Kafka)"
deadline=$((SECONDS + TIMEOUT_SECONDS))
while :; do
  call POST /api/rides/proposals "$DRIVER_TOKEN" "$(jq -nc --arg id "$RIDE_ID" '{rideRequestId:$id, price:15.5}')"
  [ "$STATUS" = 200 ] && break
  [ "$STATUS" = 404 ] && [ $SECONDS -lt $deadline ] || fail "send offer"
  sleep 1
done
PROPOSAL_ID=$(jq -er .rideProposalId <<<"$BODY") || fail "no rideProposalId"
ok "offer $PROPOSAL_ID sent"

step "Rider accepts the offer"
call PUT "/api/rides/$RIDE_ID/accept" "$RIDER_TOKEN" "$(jq -nc --arg id "$PROPOSAL_ID" '{rideProposalId:$id}')"
expect_status 200 "accept offer"
[ "$(jq -r .status <<<"$BODY")" = ACCEPTED ] || fail "accepted ride is not ACCEPTED"
ok "ride is ACCEPTED"

step "Ride ends BOOKED (driver-service books it, ride-service consumes the booked event)"
BODY=""; STATUS=""
deadline=$((SECONDS + TIMEOUT_SECONDS))
while :; do
  ride_status=$(docker compose -f "$COMPOSE_FILE" exec -T db sh -c \
    "psql -U \"\$POSTGRES_USER\" -d rides -tAc \"SELECT status FROM ride_requests WHERE ride_request_id = '$RIDE_ID'\"" \
    2>/dev/null | tr -d '[:space:]') || ride_status=""
  [ "$ride_status" = BOOKED ] && break
  [ $SECONDS -lt $deadline ] || fail "ride $RIDE_ID is '${ride_status:-?}', expected BOOKED after ${TIMEOUT_SECONDS}s"
  sleep 1
done
ok "ride $RIDE_ID is BOOKED"

printf '\nSMOKE TEST PASSED\n'
