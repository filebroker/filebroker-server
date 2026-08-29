#!/usr/bin/env bash
set -euo pipefail

env_args=()

for env_file in .env .env.secret .env.local .env.docker; do
    if [[ -f "${env_file}" ]]; then
        env_args+=(--env-file "${env_file}")
    fi
done

profile_args=()

if [[ "${1:-}" == "--local-db" ]]; then
    profile_args+=(--profile local-db)
elif [[ $# -gt 0 ]]; then
    echo "Usage: $0 [--local-db]"
    exit 1
fi

docker compose \
    "${env_args[@]}" \
    "${profile_args[@]}" \
    -f compose.yml \
    up \
    -d \
    --build \
    --wait
