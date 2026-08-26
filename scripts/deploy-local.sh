#!/usr/bin/env bash
set -euo pipefail

env_args=()

for env_file in .env .env.secret .env.local .env.docker; do
    if [[ -f "${env_file}" ]]; then
        env_args+=(--env-file "${env_file}")
    fi
done

docker compose \
    "${env_args[@]}" \
    -f compose.yml \
    up \
    -d \
    --build \
    --wait
