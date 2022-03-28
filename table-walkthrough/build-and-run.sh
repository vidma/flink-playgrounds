docker-compose down  --volumes &

DOCKER_BUILDKIT=1 docker-compose build &&  docker-compose up
