package org.apache.flink.integration.kensu;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.source.ContinuousFileMonitoringFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.runtime.operators.sink.SinkOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.ArrayList;
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

    static void logInfo(String msgFmt, String... params){
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
        // EmbeddedJobClient - null (in current CDH codebase)
        Pipeline p = jobClient.getPipeline();
        if (p == null){
            logInfo("onJobSubmitted got NULL pipeline");
            logInfo("onJobSubmitted: jobClient="+ jobClient.toString() + jobClient.getClass().toString());
        } else if (p instanceof StreamGraph){
            streamGraph = (StreamGraph) p;
        } else {
            logInfo("onJobSubmitted got pipeline that is not streamGraph: {}\n{}",
                    p.getClass().toString(), p.toString());
        }
        // FIXME: delete - seem like we don't need this anymore! cool?
        // StreamGraph streamGraph = (StreamGraph) jobClient.getPipeline();
        try {
            if (streamGraph != null) {
                logInfo("Collecting metadata for a new Flink Application: {}", streamGraph.getJobName());
                flinkApp = createAppEntity(streamGraph, jobClient.getJobID());
                String str = "FlinkAPP: " + flinkApp;
                addInputsOutputs(streamGraph);
            }
            else
                flinkApp = "???";

            //notifyEntities(Collections.singletonList(new HookNotification.EntityCreateRequestV2(getUser(), entity)), null);
        } catch (Exception e) {
            throw new RuntimeException("Atlas hook is unable to process the application.", e);
        }
//        if (throwable != null) {
//            LOG.error("Job failed to submit", throwable);
//            return;
//        }
    }

    @Override
    public void onJobExecuted(@Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {
        // FIXME: could wait until here... so it's all initialized...?
        // FIXME: what's in the result!
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
        logInfo("found sources:");
        sources.forEach(KensuFlinkHook::logSource);
        logInfo("found sinks:");
        sinks.forEach(KensuFlinkHook::logSink);
    }

    public static void logVarWithType(String varName, Object s) {
        String msg = varName;
        if (!varName.equals("")){
            varName += ":";
        }
        if (s != null) {
            msg += s.toString() + "[class=" + s.getClass().toString() + "]";
        } else {
            msg += "<null>";
        }
        KensuFlinkHook.logInfo(msg);

    }

    public static void logVarWithType(Object s) {
        logVarWithType("", s);
    }

    static void logSink(StreamNode s) {
        logVarWithType("sink", s);
        logVarWithType("sink input format", s.getInputFormat());
        logVarWithType("sink output format", s.getOutputFormat());
    }

    static void logSource(StreamNode s) {
        logVarWithType("source", s);
        logVarWithType("source input format", s.getInputFormat());
        logVarWithType("source output format", s.getOutputFormat());
        s.toString();
    }

    private void addSourceEntities(List<StreamNode> sources) {
        List<String> inputs = new ArrayList<>();

        for (StreamNode source : sources) {
            StreamOperator<?> sop = source.getOperator();
            if (sop instanceof StreamSource) {
                StreamSource<?, ?> sourceOperator = (StreamSource<?, ?>) source.getOperator();
                SourceFunction<?> sourceFunction = sourceOperator.getUserFunction();
                List<String> dsEntities = createSourceEntity(sourceFunction);
                inputs.addAll(dsEntities);
            } else {
                logInfo("source.getOperator() is not StreamSource: "+ sop.toString());
            }
        }

        // FIXME: flinkApp.setRelationshipAttribute("inputs", AtlasTypeUtil.getAtlasRelatedObjectIds(inputs, RELATIONSHIP_DATASET_PROCESS_INPUTS));
    }

    private List<String> createSourceEntity(SourceFunction<?> sourceFunction) {

        List<String> ret = new ArrayList<>();

        String sourceClass = sourceFunction.getClass().getName();
        logInfo("sourceFunction:");
        KensuFlinkHook.logVarWithType(sourceFunction);

        if (sourceClass.equals("org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer")) {
            KafkaEntities.addKafkaSourceEntity((FlinkKafkaConsumer<?>) sourceFunction, ret, getMetadataNamespace());
        } else if (sourceClass.equals("org.apache.flink.streaming.api.functions.source.ContinuousFileMonitoringFunction")) {
            FileEntities.addMonitorSourceEntity((ContinuousFileMonitoringFunction<?>) sourceFunction, ret, getMetadataNamespace());
        } else {
            logInfo("unknown source entity, sourceClass="+sourceClass);
        }
        //ret.forEach(this);
        return ret;
    }

    private void addSinkEntities(List<StreamNode> sinks) {
        List<String> outputs = new ArrayList<>();
        for (StreamNode sink : sinks) {
            // Blink changes something?
            StreamOperator<?> sinkOp = sink.getOperator();
            logInfo("addSinkEntities - sinkOp = " + sinkOp.toString() + "[class="+sinkOp.getClass().toString()+"]");
            if (sinkOp instanceof StreamSink) {
                List<String> dsEntities = createSinkEntity(((StreamSink<?>)sinkOp).getUserFunction(), sink);
                outputs.addAll(dsEntities);
            } else if (sinkOp instanceof SinkOperator) {
                // FIXME: maybe use reflection here so not to force dependency, or is it fine ?
                SinkOperator sinkOperator = (SinkOperator)sinkOp;
                List<String> dsEntities = createSinkEntity(sinkOperator.getUserFunction(), sink);
                outputs.addAll(dsEntities);
            } else {
                logInfo("sink.getOperator() is not StreamSink: "+ sinkOp.toString());
            }

        }

        //flinkApp.setRelationshipAttribute("outputs", AtlasTypeUtil.getAtlasRelatedObjectIds(outputs, RELATIONSHIP_PROCESS_DATASET_OUTPUTS));
    }

    private List<String> createSinkEntity(SinkFunction<?> sinkFunction, StreamNode sink) {
        List<String> ret = new ArrayList<>();
        String sinkClass = sinkFunction.getClass().getName();
        logInfo("found sinkClass="+sinkClass);
        if (sinkClass.equals("org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer")) {
            KafkaEntities.addKafkaSinkEntity((FlinkKafkaProducer<?>) sinkFunction, ret, getMetadataNamespace());
        } else if (sinkClass.equals("org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink")) {
            FileEntities.addStreamFileSinkEntity((StreamingFileSink<?>) sinkFunction, ret, getMetadataNamespace());
        } else if (JdbcEntities.matchesSink(sinkFunction, sinkClass)) {
            JdbcEntities.addSinkEntity(sinkFunction, sink, ret);
        } else {
            logInfo("unknown sink entity, sinkClass="+sinkClass);
        }
        //ret.forEach(e -> e.setAttribute(AtlasClient.OWNER, getUser()));
        ret.forEach(KensuFlinkHook::logInfo);
        return ret;
    }

    // Atlas thing, so we don't care?
    private String getMetadataNamespace(){
        return "";
    }

}