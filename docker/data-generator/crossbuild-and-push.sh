# docker buildx create --use

source ../../detect_platform.sh

# it seems we must push the cross-built image directly, as exporting seem unsupported
# https://docs.docker.com/engine/reference/commandline/buildx_build/#output
DOCKER_BUILDKIT=1 docker buildx build \
  --platform=linux/amd64,linux/arm64 \
  --tag kensuio/kensu-flink-collector:table-walkthrough-kafka-gen \
  --target table-walkthrough-kafka-gen \
  --push \
  .
