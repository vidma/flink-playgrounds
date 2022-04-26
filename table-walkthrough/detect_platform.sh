if [[ "$(uname -m)" == "arm64" ]]; then
    echo "ARM processor detected"
    export MYSQL_IMAGE="arm64v8/mysql:8.0.28-oracle"
    export MAVEN_IMAGE="arm64v8/maven:3.8-jdk-8-slim"
else
    echo "x86 processor detected"
    export MYSQL_IMAGE="mysql:8.0.19"
    export MAVEN_IMAGE="maven:3.8-openjdk-8-slim"
fi
