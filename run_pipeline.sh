#!/bin/bash
set -e

TENANT_ARG=${TENANT_ARG:-}
DB_DIR_ARG=${DB_DIR_ARG:-}
CONFIG_FILE_PATH=${CONFIG_PATH:-config/config.json}
CONFIG_ARG="--config $CONFIG_FILE_PATH"

[ -n "$TENANT_ARG" ] && TENANT_ARG="--tenant $TENANT_ARG"
[ -n "$DB_DIR_ARG" ] && DB_DIR_ARG="--db-dir $DB_DIR_ARG"

PAUSE_INTERVAL="${PAUSE_INTERVAL:-3h}"

if [ ! -f $CONFIG_FILE_PATH ]; then
  cp bak/config.example.json $CONFIG_FILE_PATH.rename
fi

while [ ! -s $CONFIG_FILE_PATH ]; do
  echo "Configuration file not found or empty: $CONFIG_FILE_PATH"
  echo "An example has been copied. Please edit it, then restart this script."
  sleep 30
done

while true; do
  echo "$(date '+%Y-%m-%d %H:%M:%S') Running document gatherer..."
  python therefore_document_gatherer.py $TENANT_ARG $DB_DIR_ARG $CONFIG_ARG

  echo "$(date '+%Y-%m-%d %H:%M:%S') Running document processor..."
  python therefore_document_processor.py $TENANT_ARG $DB_DIR_ARG $CONFIG_ARG

  echo "$(date '+%Y-%m-%d %H:%M:%S') Pipeline run complete. Next run after $PAUSE_INTERVAL..."
  sleep $PAUSE_INTERVAL
done
