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

ARG MAVEN_IMAGE=maven:3.8-openjdk-8-slim
FROM --platform=$BUILDPLATFORM ${MAVEN_IMAGE} AS tbl-walkthrough-builder

COPY --from=kensuio/kensu-flink-collector:cdh-latest /opt/flink/lib/kensu-flink-collector.jar /opt/lib/kensu-flink-collector.jar
COPY ./pom.xml /opt/pom.xml
COPY ./.scalafmt.conf /opt/.scalafmt.conf
COPY ./src /opt/src

# faster rebuilds (~10x?)
WORKDIR /opt
RUN --mount=type=cache,target=/root/.m2,rw --mount=type=cache,target=/root/.sbt,rw  mvn clean install -Dmaven.test.skip

FROM --platform=$TARGETPLATFORM kensuio/kensu-flink-collector:cdh-latest AS cdh-table-walkthrough

COPY --from=tbl-walkthrough-builder /opt/target/spend-report-*.jar /opt/flink/usrlib/spend-report.jar
