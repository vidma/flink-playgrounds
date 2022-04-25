#docker-compose down  --volumes
#DOCKER_BUILDKIT=1 docker-compose build

DOCKER_BUILDKIT=1 docker build \
  --tag kensuio/kensu-flink-collector:cdh-latest \
  --target kensu-flink-collector-cdh \
  .
