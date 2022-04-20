package org.apache.flink.integration.kensu;

import org.apache.flink.connector.jdbc.internal.GenericJdbcSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.flink.integration.kensu.KensuFlinkHook.logInfo;

public class JdbcEntities {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcEntities.class);

    public static boolean matchesSink(SinkFunction<?> sinkFunction, String sinkClass) {
        return sinkClass.equals("org.apache.flink.connector.jdbc.internal.GenericJdbcSinkFunction");
    }

    public static GenericJdbcSinkFunction extractSinkFunction(SinkFunction<?> sinkFunction){
        if (sinkFunction instanceof GenericJdbcSinkFunction){
            return (GenericJdbcSinkFunction) sinkFunction;
        }
        return null;
    }

    public static void addSourceEntity(FlinkKafkaConsumer kafkaSource, List<String> ret, String metadataNamespace) {
        logInfo("JdbcEntitiesSource:"+kafkaSource.toString() + ":\n");
    }

    public static void addSinkEntity(SinkFunction<?> sinkFunction, StreamNode sinkNode, List<String> ret) {
        logInfo("JdbcEntitiesSink node:"+sinkNode.getOperatorName() + ":\n");
        GenericJdbcSinkFunction<?> jdbcSink = extractSinkFunction(sinkFunction);
        if (jdbcSink == null) return;

        JdbcParser$.MODULE$.addSinkEntity(sinkFunction, sinkNode, ret);
    }

    private static String getKafkaTopicQualifiedName(String metadataNamespace, String topicName) {
        return String.format("%s@%s", topicName.toLowerCase(), metadataNamespace);
    }
}
