#!/usr/bin/env sh
# Register/Update the Postgres CDC connector. Run as a one-shot init container in docker-compose.
set -eu

CONNECT_URL="${CONNECT_URL:-http://kafka-connect:8083}"

echo "Waiting for Kafka Connect at ${CONNECT_URL}..."
until curl -s -o /dev/null -w "%{http_code}" "${CONNECT_URL}/connectors" | grep -q "200"; do
  sleep 3
done

echo "Registering pg-crypto-prices-connector..."
# POST /connectors takes the full {"name": ..., "config": {...}} envelope.
# 409 means it's already registered, which is fine on restarts.
HTTP_CODE=$(curl -s -o /tmp/resp.json -w "%{http_code}" -X POST "${CONNECT_URL}/connectors" \
  -H "Content-Type: application/json" \
  -d @/config/pg-connector.json)

if [ "$HTTP_CODE" = "201" ] || [ "$HTTP_CODE" = "409" ]; then
  echo "Connector registered (HTTP $HTTP_CODE)."
else
  echo "Connector registration failed (HTTP $HTTP_CODE):"
  cat /tmp/resp.json
  exit 1
fi
