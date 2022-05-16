################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

# flink 1.14.3 is lowest with ARM8 image
FROM --platform=$TARGETPLATFORM apache/flink:1.14.4-scala_2.12-java8 AS cdh-flink
# Replace flink dist by cloudera version of flink, see:
# https://repository.cloudera.com/artifactory/libs-release-local/org/apache/flink/flink-dist_2.12/1.13.2-csa1.5.3.1/
# https://hub.docker.com/layers/apache/flink/1.13.1-java8/images/sha256-f9761595dc8cee49c0c63f5a8658cde47dc5e7b3aef06cd5d3b8d787d68f4bc1?context=explore

# FIXME: may need separate image for that, and upgrade flink in other images too?
ENV CDHVER="1.14.0-csa1.6.2.0"
ENV FLINK_TGZ_URL="https://repository.cloudera.com/artifactory/libs-release-local/org/apache/flink/flink-dist_2.12/${CDHVER}/flink-dist_2.12-${CDHVER}.tar.gz"
WORKDIR /opt/flink
RUN rm -Rf /opt/flink/*
RUN wget -O /tmp/flink.tgz "$FLINK_TGZ_URL" && \
    tar -xf /tmp/flink.tgz --strip-components=1 && \
    rm /tmp/flink.tgz && \
    chown -R flink:flink .

# remove extra uneeded jars (present only in CDH distro) which currently cause problems \
#    when some other CDH/hadoop jars are not installed
RUN rm -f lib/flink-azure-fs-hadoop-${CDHVER}.jar
RUN rm -f lib/flink-s3-fs-hadoop-${CDHVER}.jar
RUN rm -f lib/flink-kerberos-auth-handler-1.0.jar
RUN rm -f lib/flink-metrics-kafka-${CDHVER}.jar
RUN rm -f gateway-cloud-bindings-shaded-${CDHVER}.jar

# Download connector libraries
RUN wget -P /opt/flink/lib/ https://repository.cloudera.com/artifactory/cloudera-repos/org/apache/flink/flink-sql-connector-kafka_2.12/${CDHVER}/flink-sql-connector-kafka_2.12-${CDHVER}.jar && \
    wget -P /opt/flink/lib/ https://repository.cloudera.com/artifactory/libs-release-local/org/apache/flink/flink-connector-jdbc_2.12/${CDHVER}/flink-connector-jdbc_2.12-${CDHVER}.jar && \
    wget -P /opt/flink/lib/ https://repository.cloudera.com/artifactory/libs-release-local/org/apache/flink/flink-csv/${CDHVER}/flink-csv-${CDHVER}.jar && \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/8.0.19/mysql-connector-java-8.0.19.jar


ARG MAVEN_IMAGE=maven:3.8-openjdk-8-slim
FROM ${MAVEN_IMAGE} --platform=$BUILDPLATFORM maven:3.6-jdk-8-slim AS kensu-maven-sbt-builder

# install sbt
RUN apt-get update \
    &&  apt-get install -y --no-install-recommends \
    wget apt-transport-https curl gnupg -yqq

RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list &&  \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/scalasbt-release.gpg --import && \
     chmod 644 /etc/apt/trusted.gpg.d/scalasbt-release.gpg  \
      && apt-get update \
      && apt-get install -y --no-install-recommends \
          sbt


FROM --platform=$BUILDPLATFORM kensu-maven-sbt-builder AS kensu-scala-client-builder
# make scala sources accessible to Dockerfile
# cp -R ../../../dam-client-scala/  dam-client-scala
COPY ./dam-client-scala /opt/scala-client-src
WORKDIR /opt/scala-client-src

# see https://mvnrepository.com/artifact/org.apache.flink/flink-runtime_2.12/1.13.2-csa1.5.3.1
# see https://mvnrepository.com/artifact/org.apache.flink/flink-runtime/1.14.0-csa1.6.2.0
# flink version to be matched with one pom.xml
RUN --mount=type=cache,target=/root/.m2,rw --mount=type=cache,target=/root/.sbt,rw \
    SCALA_VERSION="2.12.7" AKKA_VERSION="2.5.21" sbt compile publishM2

FROM --platform=$BUILDPLATFORM kensu-scala-client-builder AS builder

COPY ./pom.xml /opt/pom.xml
COPY ./.scalafmt.conf /opt/.scalafmt.conf
COPY ./src /opt/src

# faster rebuilds (~10x?)
WORKDIR /opt
RUN --mount=type=cache,target=/root/.m2,rw --mount=type=cache,target=/root/.sbt,rw  mvn clean install -Dmaven.test.skip

FROM --platform=$TARGETPLATFORM cdh-flink AS kensu-flink-collector-cdh

# install kensu collector to be autodiscovered by flink
COPY --from=builder /opt/target/kensu-flink-collector-*.jar /opt/flink/lib/kensu-flink-collector.jar

# fixme: https://stackoverflow.com/questions/49530333/getting-following-class-cast-exception-while-adding-job-jar-to-flink-home-lib
# seem related to apache commons lib or kafka
# added classloader.resolve-order: parent-first , which unclear impact
