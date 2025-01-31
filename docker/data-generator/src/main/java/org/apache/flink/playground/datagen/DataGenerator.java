/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.playground.datagen;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A basic data generator for continuously writing data into a Kafka topic. */
public class DataGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(DataGenerator.class);

  private static final String KAFKA = "kafka:9092";

  private static final String TOPIC = System.getenv("KAFKA_TOPIC");

  public static void main(String[] args) {
    if (TOPIC == null) {
      throw new RuntimeException("KAFKA_TOPIC environment variable not provided!");
    }
    Producer producer = new Producer(KAFKA, TOPIC);

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  LOG.info("Shutting down");
                  producer.close();
                }));

    producer.run();
  }
}
