#!/usr/bin/env bash
set -euo pipefail

# minimal: just do the three steps + restart
docker exec -it qi-superset superset fab create-admin \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --email admin@qi.local || true

docker exec -it qi-superset superset db upgrade
docker exec -it qi-superset superset init
docker restart qi-superset