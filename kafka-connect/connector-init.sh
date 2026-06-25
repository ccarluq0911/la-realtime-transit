#!/bin/sh

CONNECTOR_URL="http://kafka-connect-worker-1:8083/connectors"
CONNECTOR_NAME="mysql-vehicle-positions-source-connector"

echo "Waiting for Kafka Connect REST API..."
for i in $(seq 1 15); do
  if curl -sf "$CONNECTOR_URL" > /dev/null 2>&1; then
    break
  fi
  echo "Kafka Connect not ready... ($i/15)"
  sleep 4
done

# Remove existing connector if present (e.g. from a previous run)
curl -sf -X DELETE "$CONNECTOR_URL/$CONNECTOR_NAME" > /dev/null 2>&1 || true

cat > /tmp/connector.json <<EOF
{
  "name": "$CONNECTOR_NAME",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "tasks.max": "3",
    "connection.url": "jdbc:mysql://${DB_HOST}:${DB_PORT}/${DB_NAME}",
    "connection.user": "${DB_USER}",
    "connection.password": "${DB_PASSWORD}",
    "table.whitelist": "vehicle_positions",
    "mode": "incrementing",
    "incrementing.column.name": "internal_id",
    "poll.interval.ms": "5000"
  }
}
EOF

echo "Registering connector..."
if curl -sf -X POST -H "Content-Type: application/json" \
  --data @/tmp/connector.json \
  "$CONNECTOR_URL" > /dev/null; then
  echo "Connector registered!"
else
  echo "Registration failed (table may not exist yet; connector will auto-retry)" >&2
fi
