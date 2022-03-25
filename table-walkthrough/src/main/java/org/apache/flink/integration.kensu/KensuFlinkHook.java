package org.apache.flink.integration.kensu;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.core.execution.DetachedJobExecutionResult;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.runtime.minicluster.MiniClusterJobClient;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

        // FIXME: there was a further wrapper using a custom class loader. do we need that?
        //  https://github.com/gyfora/atlas/commit/f0f8b94db5c86d9f424e5a8c4dfde94c1ceef352#diff-52de8bc60c8e19861b0ad1c6c7eb7145a5f733d279fbb5b380b9c77b6c8ad66b
public class KensuFlinkHook implements JobListener {

    //public static final Logger LOG = org.slf4j.LoggerFactory.getLogger(FlinkAtlasHook.class);
    private static final Logger LOG = LoggerFactory.getLogger(KensuFlinkHook.class);

    public static final String RELATIONSHIP_DATASET_PROCESS_INPUTS = "dataset_process_inputs";
    public static final String RELATIONSHIP_PROCESS_DATASET_OUTPUTS = "process_dataset_outputs";

    private String flinkApp;
//    private AtlasEntity.AtlasEntitiesWithExtInfo entity;

    private static void logInfo(String msgFmt, String... params){
        String msg = String.format(msgFmt, params);
        System.out.println("org.apache.flink.integration.kensu - logInfo - " + msg);
        LOG.warn(msg);
    }

    private String getUser(){
        return "unknown";
    }

    @Override
    public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {
        logInfo("in KensuFlinkHook.onJobSubmitted");
        if (jobClient == null){
            logInfo("onJobSubmitted got NULL jobClient");
            return;
        }
        StreamGraph streamGraph = null;
        Pipeline p = jobClient.getPipeline();
        if (p instanceof StreamGraph){
            streamGraph = (StreamGraph) p;
        }
        if (throwable != null) {
            LOG.error("Job failed to submit", throwable);
            return;
        }
        // FIXME: delete - seem like we don't need this anymore! cool?
        // StreamGraph streamGraph = (StreamGraph) jobClient.getPipeline();

        try {
            if (jobClient != null && streamGraph != null) {
                logInfo("Collecting metadata for a new Flink Application: {}", streamGraph.getJobName());
                flinkApp = createAppEntity(streamGraph, jobClient.getJobID());
                String str = "FlinkAPP: " + flinkApp;
                BufferedWriter writer = new BufferedWriter(new FileWriter("/tmp/kensu_flink.log", true));
                writer.append(' ');
                writer.append(str);
                writer.close();
                addInputsOutputs(streamGraph);
            }
            else
                flinkApp = "???";

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
        logInfo("ENDED flink APP:" + flinkApp +
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
        logInfo("STARTED APP: " + appInfo);
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

        ret.forEach(LOG::warn);
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
        ret.forEach(KensuFlinkHook::logInfo);
        return ret;
    }

    private String getMetadataNamespace(){
        // FIXME
        return "?";
    }

}