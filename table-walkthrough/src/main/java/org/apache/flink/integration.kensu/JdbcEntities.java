package org.apache.flink.integration.kensu;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.internal.AbstractJdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.GenericJdbcSinkFunction;
import org.apache.flink.connector.jdbc.internal.JdbcBatchingOutputFormat;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.runtime.operators.sink.SinkOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.List;

import static org.apache.flink.integration.kensu.KensuFlinkHook.logInfo;
import static org.apache.flink.integration.kensu.KensuFlinkHook.logVarWithType;

public class JdbcEntities {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcEntities.class);

    public static boolean matchesSink(SinkFunction<?> sinkFunction, String sinkClass) {
        return sinkClass.equals("org.apache.flink.connector.jdbc.internal.GenericJdbcSinkFunction");
    }

    private static GenericJdbcSinkFunction extractSinkFunction(SinkFunction<?> sinkFunction){
        if (sinkFunction instanceof GenericJdbcSinkFunction){
            return (GenericJdbcSinkFunction) sinkFunction;
        }
        return null;
    }

    public static void addSourceEntity(FlinkKafkaConsumer kafkaSource, List<String> ret, String metadataNamespace) {
        logInfo("JdbcEntitiesSource:"+kafkaSource.toString() + ":\n");
        // FIXME: extract using reflection?
//        KafkaTopicsDescriptor topicsDescriptor = kafkaSource.getTopicsDescriptor();
//        Properties kafkaProps = kafkaSource.getProperties();
//
//        List<String> topics = topicsDescriptor.isFixedTopics() ? topicsDescriptor.getFixedTopics() : Collections.singletonList(topicsDescriptor.getTopicPattern().toString());
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

    public static void addSinkEntity(SinkFunction<?> sinkFunction, StreamNode sink, List<String> ret) {
        logInfo("JdbcEntitiesSink:"+sinkFunction.toString() + ":\n");
        GenericJdbcSinkFunction<?> jdbcSink = extractSinkFunction(sinkFunction);
        if (jdbcSink == null) return;
        //sink.toString();

        // RunTimeCOntext
        logInfo("addSinkEntity - jdbcSink:");
        logVarWithType(jdbcSink);

        logInfo("jdbc:: sink - getOutputFormat:");
        logVarWithType(sink.getOutputFormat());

        AbstractJdbcOutputFormat<?> outputFormat = new ReflectHelpers<AbstractJdbcOutputFormat<?>>().reflectGetField(jdbcSink, "outputFormat");
        logInfo("GenericJdbcSinkFunction.outputFormat from reflect:");
        logVarWithType(outputFormat);
        if (outputFormat != null && outputFormat instanceof JdbcBatchingOutputFormat){
            JdbcBatchingOutputFormat bOutFormat  = (JdbcBatchingOutputFormat) outputFormat;
            //outputFormat.connectionProvider
            JdbcConnectionProvider  jConnProv = new ReflectHelpers<JdbcConnectionProvider>().reflectGetField(outputFormat, "connectionProvider");
            if (jConnProv != null  && jConnProv instanceof SimpleJdbcConnectionProvider){
                JdbcConnectionOptions jdbcOptions =  new ReflectHelpers<JdbcConnectionOptions>().reflectGetField(jConnProv, "jdbcOptions");
                if (jdbcOptions != null) {
                    logInfo("KENSU JDBC CONN: getDbURL=%s, driver=%s", jdbcOptions.getDbURL(), jdbcOptions.getDriverName());
                }
            }
        }
        // FIXME: still no clue yet how to get schema cleanly....
    }

    private static String getKafkaTopicQualifiedName(String metadataNamespace, String topicName) {
        return String.format("%s@%s", topicName.toLowerCase(), metadataNamespace);
    }
}
