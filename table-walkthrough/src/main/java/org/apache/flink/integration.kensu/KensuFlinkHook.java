package org.apache.flink.integration.kensu;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.execution.DetachedJobExecutionResult;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.source.ContinuousFileMonitoringFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

//import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

        // FIXME: there was a further wrapper using a custom class loader. do we need that?
        //  https://github.com/gyfora/atlas/commit/f0f8b94db5c86d9f424e5a8c4dfde94c1ceef352#diff-52de8bc60c8e19861b0ad1c6c7eb7145a5f733d279fbb5b380b9c77b6c8ad66b
public class KensuFlinkHook implements JobListener {

    //public static final Logger LOG = org.slf4j.LoggerFactory.getLogger(FlinkAtlasHook.class);

    public static final String RELATIONSHIP_DATASET_PROCESS_INPUTS = "dataset_process_inputs";
    public static final String RELATIONSHIP_PROCESS_DATASET_OUTPUTS = "process_dataset_outputs";

    private String flinkApp;
//    private AtlasEntity.AtlasEntitiesWithExtInfo entity;

    private void logInfo(String msgFmt, String... params){
        String msg = String.format(msgFmt, params);
        System.out.println(msg);
    }

    private String getUser(){
        return "unknown";
    }

    @Override
    public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamGraph streamGraph = senv.getStreamGraph();
        System.out.println("StreamExecutionEnvironment.getExecutionPlan:" + senv.getExecutionPlan());

        if (throwable != null) {
            return;
        }
        // FIXME: delete - seem like we don't need this anymore! cool?
        // StreamGraph streamGraph = (StreamGraph) jobClient.getPipeline();
        logInfo("Collecting metadata for a new Flink Application: {}", streamGraph.getJobName());

        try {

            flinkApp = createAppEntity(streamGraph, jobClient.getJobID());
            addInputsOutputs(streamGraph);

            //notifyEntities(Collections.singletonList(new HookNotification.EntityCreateRequestV2(getUser(), entity)), null);
        } catch (Exception e) {
            throw new RuntimeException("Atlas hook is unable to process the application.", e);
        }
    }

    @Override
    public void onJobExecuted(@Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {
//        if (throwable != null || jobExecutionResult instanceof DetachedJobExecutionResult) {
//            return;
//        }
        System.out.println("ENDED flink APP:" + flinkApp +
                "\n------\nendTime:" +new Date(System.currentTimeMillis()));
        //flinkApp.setAttribute("endTime", );
        //notifyEntities(Collections.singletonList(new HookNotification.EntityCreateRequestV2(getUser(), entity)), null);
    }

    private String createAppEntity(StreamGraph graph, JobID jobId) {
        String appInfo = "\nid:" +jobId.toString() +
                "\nNAME:" + graph.getJobName() +
                "\nstartTime:" + new Date(System.currentTimeMillis()) +
                //flinkApp.setAttribute(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, getMetadataNamespace());
                "\nOWNER:" + getUser();
        System.out.println("STARTED APP: " + appInfo);
        return appInfo;
    }

    private void addInputsOutputs(StreamGraph streamGraph) {
        List<StreamNode> sources = streamGraph
                .getSourceIDs()
                .stream()
                .map(streamGraph::getStreamNode)
                .collect(Collectors.toList());

        addSourceEntities(sources);

        List<StreamNode> sinks = streamGraph
                .getSinkIDs()
                .stream()
                .map(streamGraph::getStreamNode)
                .collect(Collectors.toList());

        addSinkEntities(sinks);
    }

    private void addSourceEntities(List<StreamNode> sources) {
        List<String> inputs = new ArrayList<>();

        for (StreamNode source : sources) {
            StreamSource<?, ?> sourceOperator = (StreamSource<?, ?>) source.getOperator();
            SourceFunction<?> sourceFunction = sourceOperator.getUserFunction();

            List<String> dsEntities = createSourceEntity(sourceFunction);
            inputs.addAll(dsEntities);
        }

        // FIXME: flinkApp.setRelationshipAttribute("inputs", AtlasTypeUtil.getAtlasRelatedObjectIds(inputs, RELATIONSHIP_DATASET_PROCESS_INPUTS));
    }

    private List<String> createSourceEntity(SourceFunction<?> sourceFunction) {

        List<String> ret = new ArrayList<>();

        String sourceClass = sourceFunction.getClass().getName();

        if (sourceClass.equals("org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer")) {
            KafkaEntities.addKafkaSourceEntity((FlinkKafkaConsumer<?>) sourceFunction, ret, getMetadataNamespace());
        } else if (sourceClass.equals("org.apache.flink.streaming.api.functions.source.ContinuousFileMonitoringFunction")) {
            FileEntities.addMonitorSourceEntity((ContinuousFileMonitoringFunction<?>) sourceFunction, ret, getMetadataNamespace());
        }

        ret.forEach(System.out::println);
        return ret;
    }

    private void addSinkEntities(List<StreamNode> sinks) {
        List<String> outputs = new ArrayList<>();
        for (StreamNode sink : sinks) {
            StreamSink<?> sinkOperator = (StreamSink<?>) sink.getOperator();
            SinkFunction<?> sinkFunction = sinkOperator.getUserFunction();

            List<String> dsEntities = createSinkEntity(sinkFunction);
            outputs.addAll(dsEntities);

        }

        //flinkApp.setRelationshipAttribute("outputs", AtlasTypeUtil.getAtlasRelatedObjectIds(outputs, RELATIONSHIP_PROCESS_DATASET_OUTPUTS));
    }

    private List<String> createSinkEntity(SinkFunction<?> sinkFunction) {

        List<String> ret = new ArrayList<>();

        String sinkClass = sinkFunction.getClass().getName();

        if (sinkClass.equals("org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer")) {
            KafkaEntities.addKafkaSinkEntity((FlinkKafkaProducer<?>) sinkFunction, ret, getMetadataNamespace());
        } else if (sinkClass.equals("org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink")) {
            FileEntities.addStreamFileSinkEntity((StreamingFileSink<?>) sinkFunction, ret, getMetadataNamespace());
        }

        //ret.forEach(e -> e.setAttribute(AtlasClient.OWNER, getUser()));
        ret.forEach(System.out::println);
        return ret;
    }

    private String getMetadataNamespace(){
        // FIXME
        return "?";
    }

}