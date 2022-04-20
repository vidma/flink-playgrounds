package org.apache.flink.integration.kensu;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicsDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.integration.kensu.KensuFlinkHook.logInfo;

public class KafkaEntities {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaEntities.class);

    public static void addKafkaSourceEntity(FlinkKafkaConsumer kafkaSource, List<String> ret, String metadataNamespace) {
        logInfo("kafkaSource:"+kafkaSource.toString() + ":\n");
        // FIXME: extract using reflection?
        logInfo("kafkaSource.getProducedType():"+ kafkaSource.getProducedType().toString());
        //kafkaSource.getDeserializer();
        KafkaTopicsDescriptor topicsDescriptor = kafkaSource.getTopicsDescriptor();
        Properties kafkaProps = kafkaSource.getProperties();

        List<String> topics = topicsDescriptor.isFixedTopics() ? topicsDescriptor.getFixedTopics() : Collections.singletonList(topicsDescriptor.getTopicPattern().toString());
        String uri = kafkaProps.getProperty("bootstrap.servers");

        for (String topic : topics) {
            String e = String.format("KENSU KafkaSource(topic=%s, uri=%s, TopicQualifiedName=%s)",
                    topic, uri, getKafkaTopicQualifiedName(metadataNamespace, topic));
            logInfo(e);
            ret.add(e);
        }
    }

    public static void addKafkaSinkEntity(FlinkKafkaProducer kafkaSink, List<String> ret, String metadataNamespace) {
        logInfo("kafkaSink:"+kafkaSink.toString() + ":\n");
//        KafkaTopicsDescriptor topicsDescriptor = kafkaSink.getTopicsDescriptor();
//        Properties kafkaProps = kafkaSink.getProperties();
//
//        List<String> topics = new ArrayList<>();
//        if (topicsDescriptor.isFixedTopics()) {
//            topics.addAll(topicsDescriptor.getFixedTopics());
//        } else {
//            topics.add(topicsDescriptor.getTopicPattern().toString());
//        }
//
//        String uri = kafkaProps.getProperty("bootstrap.servers");
//
//        for (String topic : topics) {
//            AtlasEntity e = new AtlasEntity(FlinkDataTypes.KAFKA_TOPIC.getName());
//
//            e.setAttribute("topic", topic);
//            e.setAttribute("uri", uri);
//            e.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, getKafkaTopicQualifiedName(metadataNamespace, topic));
//            e.setAttribute(AtlasClient.NAME, topic);
//            ret.add(e);
//        }
    }

    private static String getKafkaTopicQualifiedName(String metadataNamespace, String topicName) {
        return String.format("%s@%s", topicName.toLowerCase(), metadataNamespace);
    }
}
