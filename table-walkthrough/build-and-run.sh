export KENSU_INGESTION_URL
export KENSU_AUTH_TOKEN

source ./detect_platform.sh

docker-compose down  --volumes &

DOCKER_BUILDKIT=1 docker-compose build &&  docker-compose up --force-recreate
