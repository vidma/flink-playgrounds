docker-compose down jobmanager --volumes jobmanager

DOCKER_BUILDKIT=1 docker-compose build && docker-compose up jobmanager
