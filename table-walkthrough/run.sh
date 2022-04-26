export KENSU_INGESTION_URL
export KENSU_AUTH_TOKEN

source ./detect_platform.sh

docker-compose down  --volumes
docker pull kensuio/kensu-flink-collector:cdh-table-walkthrough
docker-compose up --force-recreate
