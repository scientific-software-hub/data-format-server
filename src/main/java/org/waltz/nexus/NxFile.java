package org.waltz.nexus;


import io.jhdf.HdfFile;
import io.jhdf.WritableDatasetImpl;
import io.jhdf.WritableGroupImpl;
import io.jhdf.WritableHdfFile;
import io.jhdf.api.WritableGroup;
import io.jhdf.api.WritableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static org.waltz.nexus.NxdlParser.ROOT_PATH;

public class NxFile {
    private final Logger logger = LoggerFactory.getLogger(NxFile.class);

    private final WritableHdfFile h5File;
    final ConcurrentMap<String, WritableNode> nxPathMapping = new ConcurrentHashMap<>();


    public NxFile(String path, String template) throws Exception {
        h5File = HdfFile.write(Paths.get(path));
        nxPathMapping.put(ROOT_PATH, h5File);

        var parser = new NxdlParser(template);
        parser.parse();
        var iterator = parser.iterator();
        while (iterator.hasNext()) {
            iterator.next().create(this);
        }
    }

    private NxFile(WritableHdfFile h5File) {
        this.h5File = h5File;
    }

    public static NxFile open(String path) {
        return new NxFile(HdfFile.write(Paths.get(path)));
    }

    private void write(String nxPath, Object v){
        var pgroup = (WritableGroup) nxPathMapping.get(nxPath.substring(0, nxPath.lastIndexOf("/")));
        if(pgroup == null) throw new IllegalArgumentException("No such nxPath: " + nxPath);
        var dataset = new WritableDatasetImpl(v, nxPath.substring(nxPath.lastIndexOf("/")), pgroup);
        nxPathMapping.putIfAbsent(nxPath, dataset);
    }

    private void append(String nxPath, Object newData) {
        throw new UnsupportedOperationException();
    }


    public void write(String nxPath, String v, boolean append) {
        if(append){
            append(nxPath, v);
        } else {
            write(nxPath, new String[]{v});
        }


        logger.debug("Updated dataset: {}  with value: {}", nxPath, v);
    }

    public void write(String nxPath, double v, boolean append) {
        if(append){
            append(nxPath, v);
        } else {
            write(nxPath, new double[]{v});
        }


        logger.debug("Updated dataset: {}  with value: {}", nxPath, v);

    }

    public void write(String nxPath, float v, boolean append) throws Exception {
        if(append){
            append(nxPath, v);
        } else {
            write(nxPath, new float[]{v});
        }


        logger.debug("Updated dataset: {}  with value: {}", nxPath, v);

    }

    public void write(String nxPath, long v, boolean append) throws Exception {
        if(append){
            append(nxPath, v);
        } else {
            write(nxPath, new long[]{v});
        }

        logger.debug("Updated dataset: {}  with value: {}", nxPath, v);

    }

    public void write(String nxPath, int v, boolean append) throws Exception {
        if(append){
            append(nxPath, v);
        } else {
            write(nxPath, new int[]{v});
        }

        logger.debug("Updated dataset: {}  with value: {}", nxPath, v);

    }

    public void write(String nxPath, short v, boolean append) throws Exception {
        if(append){
            append(nxPath, v);
        } else {
            write(nxPath, new short[]{v});
        }


        logger.debug("Updated dataset: {}  with value: {}", nxPath, v);

    }



    public void close() {
        h5File.close();
    }

    public String getFileName() {
        return h5File.getPath();
    }

    public void write(String nxPath, double v) throws Exception {
        write(nxPath, v, false);
    }

    public void write(String nxPath, String v) throws Exception {
        write(nxPath, v, false);
    }

    public WritableHdfFile getH5File(){
        return h5File;
    }
}
