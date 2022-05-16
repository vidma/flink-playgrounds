#docker-compose down  --volumes
#DOCKER_BUILDKIT=1 docker-compose build

source ../detect_platform.sh

# https://www.docker.com/blog/faster-multi-platform-builds-dockerfile-cross-compilation-guide/

# docker buildx create --use

# it seems we must push the cross-built image directly, as exporting seem unsupported
# https://docs.docker.com/engine/reference/commandline/buildx_build/#output
DOCKER_BUILDKIT=1 docker buildx build \
  --platform=linux/amd64,linux/arm64 \
  --tag kensuio/kensu-flink-collector:cdh-latest \
  --target kensu-flink-collector-cdh \
  --push \
  .
