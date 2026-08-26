#!/usr/bin/env bash
set -euo pipefail

compose() {
    local env_args=()

    for env_file in .env .env.secret .env.local .env.docker; do
        if [[ -f "${env_file}" ]]; then
            env_args+=(--env-file "${env_file}")
        fi
    done

    docker compose \
        "${env_args[@]}" \
        -f compose.prod.yml \
        "$@"
}

echo "Pulling latest filebroker images"
compose pull \
    filebroker-server-castor \
    filebroker-server-pollux \
    nginx

echo "Updating filebroker-server-castor"
compose up \
    -d \
    --no-deps \
    --pull never \
    --wait \
    filebroker-server-castor

echo "filebroker-server-castor is healthy"

echo "Updating filebroker-server-pollux"
compose up \
    -d \
    --no-deps \
    --pull never \
    --wait \
    filebroker-server-pollux

echo "filebroker-server-pollux is healthy"

echo "Updating filebroker client/nginx"
compose up \
    -d \
    --no-deps \
    --pull never \
    --force-recreate \
    --wait \
    nginx

echo "Deployment complete"
