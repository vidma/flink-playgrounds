#docker-compose down --volumes jobmanager

source ../detect_platform.sh

DOCKER_BUILDKIT=1 docker-compose build && docker-compose up client

