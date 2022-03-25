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

FROM maven:3.6-jdk-8-slim AS builder

COPY ./pom.xml /opt/pom.xml
COPY ./src /opt/src

# faster rebuilds (~10x?)
WORKDIR /opt
RUN --mount=type=cache,target=/root/.m2,rw  mvn clean install -Dmaven.test.skip


FROM apache/flink:1.13.1-scala_2.12-java8

# Replace flink dist by cloudera version of flink, see:
# https://repository.cloudera.com/artifactory/libs-release-local/org/apache/flink/flink-dist_2.12/1.13.2-csa1.5.3.1/
# https://hub.docker.com/layers/apache/flink/1.13.1-java8/images/sha256-f9761595dc8cee49c0c63f5a8658cde47dc5e7b3aef06cd5d3b8d787d68f4bc1?context=explore

# FIXME: may need separate image for that, and upgrade flink in other images too?
ENV FLINK_TGZ_URL="https://repository.cloudera.com/artifactory/libs-release-local/org/apache/flink/flink-dist_2.12/1.13.2-csa1.5.3.1/flink-dist_2.12-1.13.2-csa1.5.3.1.tar.gz"
WORKDIR /opt/flink
RUN rm -Rf /opt/flink/*
RUN wget -O /tmp/flink.tgz "$FLINK_TGZ_URL"; \
tar -xf /tmp/flink.tgz --strip-components=1; \
    rm flink.tgz;    \
    chown -R flink:flink

# Download connector libraries
RUN wget -P /opt/flink/lib/ https://repository.cloudera.com/artifactory/cloudera-repos/org/apache/flink/flink-sql-connector-kafka_2.12/1.13.2-csa1.5.3.1/flink-sql-connector-kafka_2.12-1.13.2-csa1.5.3.1.jar; \
    wget -P /opt/flink/lib/ https://repository.cloudera.com/artifactory/libs-release-local/org/apache/flink/flink-connector-jdbc_2.12/1.13.2-csa1.5.3.1/flink-connector-jdbc_2.12-1.13.2-csa1.5.3.1.jar; \
    wget -P /opt/flink/lib/ https://repository.cloudera.com/artifactory/libs-release-local/org/apache/flink/flink-csv/1.13.2-csa1.5.3.1/flink-csv-1.13.2-csa1.5.3.1.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/8.0.19/mysql-connector-java-8.0.19.jar;

COPY --from=builder /opt/target/spend-report-*.jar /opt/flink/usrlib/spend-report.jar

# fixme: https://stackoverflow.com/questions/49530333/getting-following-class-cast-exception-while-adding-job-jar-to-flink-home-lib
# seem related to apache commons lib or kafka
# added classloader.resolve-order: parent-first , which unclear impact
RUN echo "execution.checkpointing.interval: 10s" >> /opt/flink/conf/flink-conf.yaml; \
    echo "pipeline.object-reuse: true" >> /opt/flink/conf/flink-conf.yaml; \
    echo "pipeline.time-characteristic: EventTime" >> /opt/flink/conf/flink-conf.yaml; \
    echo "taskmanager.memory.jvm-metaspace.size: 256m" >> /opt/flink/conf/flink-conf.yaml; \
    echo "classloader.resolve-order: parent-first" >> /opt/flink/conf/flink-conf.yaml; \
    echo "execution.job-listeners: org.apache.flink.integration.kensu.KensuFlinkHook" >> /opt/flink/conf/flink-conf.yaml;

