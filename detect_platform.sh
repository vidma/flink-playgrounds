echo "Detecting platform"

PROCESSOR="$(uname -m)"
# uncomment below for testing with forced ARM platform, even on amd64
# FORCE_PROCESSOR="arm64"

if [[ ! -z "$FORCE_PROCESSOR" ]]; then
    PROCESSOR="$FORCE_PROCESSOR"
fi

if [[ "$PROCESSOR" == "arm64" ]]; then
    echo "ARM processor detected"
    export MYSQL_IMAGE="arm64v8/mysql:8.0.28-oracle"
    export MAVEN_IMAGE="arm64v8/maven:3.8-jdk-8-slim"
    export ZOOKEEPER_IMAGE="arm64v8/zookeeper:3.5.9"
    export FLINK_PLATFORM="linux/arm64/v8"
else
    echo "x86 processor detected"
    export MYSQL_IMAGE="mysql:8.0.19"
    export MAVEN_IMAGE="maven:3.8-openjdk-8-slim"
    export ZOOKEEPER_IMAGE="wurstmeister/zookeeper:3.4.6"
    export FLINK_PLATFORM="linux/amd64"
fi

# p.s. docker-compose seem to ignore platform setting!
if [[ ! -z "$FORCE_PROCESSOR" ]]; then
    export DOCKER_DEFAULT_PLATFORM="$FLINK_PLATFORM"
fi


echo "FLINK_PLATFORM: $FLINK_PLATFORM"
echo "MYSQL_IMAGE: $MYSQL_IMAGE"
echo "MAVEN_IMAGE: $MAVEN_IMAGE"
echo "ZOOKEEPER_IMAGE: $ZOOKEEPER_IMAGE"
echo "DOCKER_DEFAULT_PLATFORM: $DOCKER_DEFAULT_PLATFORM"
