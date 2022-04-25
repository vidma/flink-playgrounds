#docker-compose down  --volumes
#DOCKER_BUILDKIT=1 docker-compose build

DOCKER_BUILDKIT=1 docker build \
  --tag kensuio/cdh-kensu-flink-collector-runner \
  --target cdh-kensu-flink-collector-runner \
  .
