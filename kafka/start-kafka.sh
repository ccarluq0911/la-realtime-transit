#!/usr/bin/env bash
set -euo pipefail

: "${CONFIG_FILE:?CONFIG_FILE is required}"
: "${KAFKA_CLUSTER_ID:?KAFKA_CLUSTER_ID is required}"
: "${ROLE:?ROLE is required}"

DATA_DIR=/opt/kafka/data
mkdir -p "$DATA_DIR"

format_storage() {
  /opt/kafka/bin/kafka-storage.sh format \
    --ignore-formatted \
    --cluster-id "$KAFKA_CLUSTER_ID" \
    "$@" \
    --config "$CONFIG_FILE"
}

if [ ! -f "$DATA_DIR/meta.properties" ]; then
  case "$ROLE" in
    controller)
      : "${KAFKA_CONTROLLER_DIRECTORY_ID:?KAFKA_CONTROLLER_DIRECTORY_ID is required for controller bootstrap}"
      format_storage --initial-controllers "1@kafka-controller:9096:$KAFKA_CONTROLLER_DIRECTORY_ID"
      ;;
    broker)
      format_storage --no-initial-controllers
      ;;
    *)
      echo "ROLE must be controller or broker" >&2
      exit 1
      ;;
  esac
fi

exec /opt/kafka/bin/kafka-server-start.sh "$CONFIG_FILE"