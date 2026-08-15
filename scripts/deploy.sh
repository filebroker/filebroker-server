#!/usr/bin/env bash
set -euo pipefail

compose() {
    docker compose -f compose.prod.yml "$@"
}

echo "Pulling latest filebroker-server image"
compose pull \
    filebroker-server-castor \
    filebroker-server-pollux

echo "Updating filebroker-server-castor"
compose up \
    -d \
    --no-deps \
    --pull never \
    --wait \
    --wait-timeout 600 \
    filebroker-server-castor

echo "filebroker-server-castor is healthy"

echo "Updating filebroker-server-pollux"
compose up \
    -d \
    --no-deps \
    --pull never \
    --wait \
    --wait-timeout 600 \
    filebroker-server-pollux

echo "filebroker-server-pollux is healthy"
echo "Deployment complete"
