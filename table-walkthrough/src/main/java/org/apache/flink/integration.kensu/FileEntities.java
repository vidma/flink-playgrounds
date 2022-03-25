package org.apache.flink.integration.kensu;

//import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.source.ContinuousFileMonitoringFunction;


//FIXME: import org.apache.atlas.utils.HdfsNameServiceResolver;
//import org.apache.commons.lang.StringUtils;
//import org.apache.hadoop.fs.Path;

import java.util.List;

public class FileEntities {

    public static void addStreamFileSinkEntity(StreamingFileSink fileSink, List<String> ret, String metadataNamespace) {
       // addFileEntity(new Path(fileSink.getBucketsBuilder().getBasePath().toUri()), ret, metadataNamespace);

    }

    public static void addMonitorSourceEntity(ContinuousFileMonitoringFunction monitorSource, List<String> ret, String metadataNamespace) {

       // addFileEntity(new Path(monitorSource.getMonitoredPath()), ret, metadataNamespace);
    }

//    private static void addFileEntity(Path hdfsPath, List<AtlasEntity> ret, String metadataNamespace) {
//        final String nameServiceID = HdfsNameServiceResolver.getNameServiceIDForPath(hdfsPath.toString());
//
//        //AtlasEntity e = new AtlasEntity(FlinkDataTypes.HDFS_PATH.getName());
//
//        e.setAttribute(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, metadataNamespace);
//        e.setAttribute(AtlasClient.NAME, Path.getPathWithoutSchemeAndAuthority(hdfsPath).toString().toLowerCase());
//
//        if (StringUtils.isNotEmpty(nameServiceID)) {
//            String updatedPath = HdfsNameServiceResolver.getPathWithNameServiceID(hdfsPath.toString());
//
//            e.setAttribute("path", updatedPath);
//            e.setAttribute("nameServiceId", nameServiceID);
//            e.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, getHdfsPathQualifiedName(metadataNamespace, updatedPath));
//        } else {
//            e.setAttribute("path", hdfsPath.toString());
//            e.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, getHdfsPathQualifiedName(metadataNamespace, hdfsPath.toString()));
//        }
//        ret.add(e);
//    }

    private static String getHdfsPathQualifiedName(String metadataNamespace, String hdfsPath) {
        return String.format("%s@%s", hdfsPath.toLowerCase(), metadataNamespace);
    }
}

class HdfsNameServiceResolver {
    public static String getNameServiceIDForPath(String path) {
        return path;
    }
    // FIXME: not implemented
    public static String getPathWithNameServiceID(String path) {
        return path;
    }
}