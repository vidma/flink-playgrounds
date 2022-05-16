export KENSU_INGESTION_URL
export KENSU_AUTH_TOKEN
export STATS_COMPUTE_INTERVAL_MINUTES

source ../detect_platform.sh

docker-compose down  --volumes
docker pull kensuio/kensu-flink-collector:cdh-table-walkthrough
docker pull kensuio/kensu-flink-collector:table-walkthrough-kafka-gen

docker-compose up --force-recreate
