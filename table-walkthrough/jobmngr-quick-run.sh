docker-compose down --volumes jobmanager

DOCKER_BUILDKIT=1 docker-compose build && docker-compose up jobmanager
