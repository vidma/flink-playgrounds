export KENSU_INGESTION_URL
export KENSU_AUTH_TOKEN
export STATS_COMPUTE_INTERVAL_MINUTES

source ../detect_platform.sh

docker-compose down  --volumes &

DOCKER_BUILDKIT=1 docker-compose build &&  docker-compose up --force-recreate
