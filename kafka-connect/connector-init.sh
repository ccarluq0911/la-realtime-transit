#!/bin/sh

echo "Waiting for Kafka Connect REST API..."
for i in $(seq 1 15); do
  if curl -sf http://kafka-connect-worker-1:8083/connectors > /dev/null 2>&1; then
    echo "Registering connector..."
    curl -X POST -H "Content-Type: application/json" \
      --data @/connector.json \
      http://kafka-connect-worker-1:8083/connectors && echo "Connector registered!" && exit 0
    echo "Connector registration failed" >&2
    exit 1
  fi
  echo "Waiting... ($i/15)"
  sleep 4
done
echo "Timed out waiting for Kafka Connect" >&2
exit 1
