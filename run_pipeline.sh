#!/bin/bash
set -e

# Set from environment variables if provided
TENANT_ARG=${TENANT_ARG:-}
DB_DIR_ARG=${DB_DIR_ARG:-}

# If set, prepend the argument name
[ -n "$TENANT_ARG" ] && TENANT_ARG="--tenant $TENANT_ARG"
[ -n "$DB_DIR_ARG" ] && DB_DIR_ARG="--db-dir $DB_DIR_ARG"

PAUSE_INTERVAL="${PAUSE_INTERVAL:-3h}"

if [ ! -f /config/config.json ]; then
  cp /config/config.example.json /config/config.example.json
fi

# Wait for a non-empty config file to exist
while [ ! -s /config/config.json ]; do
  echo "Configuration file not found. An example file has been copied to the /config directory. Edit and rename this to config.json"
  echo "Waiting for a valid /config/config.json..."
  sleep 30
done

while true; do
  echo "$(date '+%Y-%m-%d %H:%M:%S') Running document gatherer..."
  python therefore_document_gatherer.py $TENANT_ARG $DB_DIR_ARG

  echo "$(date '+%Y-%m-%d %H:%M:%S') Running document processor..."
  python therefore_document_processor.py $TENANT_ARG $DB_DIR_ARG

  echo "$(date '+%Y-%m-%d %H:%M:%S') Pipeline run complete. Next run after $PAUSE_INTERVAL..."
  sleep $PAUSE_INTERVAL
done