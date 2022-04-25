export KENSU_INGESTION_URL
export KENSU_AUTH_TOKEN

docker-compose down  --volumes &
DOCKER_BUILDKIT=1 docker-compose build &&  docker-compose up --force-recreate
