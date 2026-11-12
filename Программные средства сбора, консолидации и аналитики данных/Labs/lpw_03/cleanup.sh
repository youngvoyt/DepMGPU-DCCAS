#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "$0")"
# Default is reversible: preserve all database volumes.
docker compose down --remove-orphans
echo 'Контейнеры остановлены. Данные сохранены в томах Docker.'
