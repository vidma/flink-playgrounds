//package org.apache.flink.integration.kensu;
//
//
//import org.apache.flink.configuration.IllegalConfigurationException;
//import org.apache.flink.configuration.ReadableConfig;
//import org.apache.flink.core.execution.JobListenerFactory;
//
// FIXME: JobListenerFactory not present in default flink (need to patch)
//public class FlinkKensuHookFactory implements JobListenerFactory<KensuFlinkHook> {
//
//    @Override
//    public KensuFlinkHook createFromConfig(ReadableConfig readableConfig, ClassLoader classLoader) throws IllegalConfigurationException {
//        return new KensuFlinkHook();
//    }
//
//}