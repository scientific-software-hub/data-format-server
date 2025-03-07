package org.waltz.nexus;


import io.jhdf.HdfFile;
import io.jhdf.WritableHdfFile;
import io.jhdf.api.WritableDataset;
import io.jhdf.api.WritableGroup;
import io.jhdf.api.WritableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.lang.reflect.Array;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.waltz.nexus.NxdlParser.ROOT_PATH;

/**
 * This class is {@link NotThreadSafe} and must be isolated in a dedicated thread
 *
 */
@NotThreadSafe
public class NxFile {
    private final Logger logger = LoggerFactory.getLogger(NxFile.class);

    private final WritableHdfFile h5File;
    final ConcurrentMap<String, WritableNode> nxPathMapping = new ConcurrentHashMap<>();
    private final long open;


    public NxFile(String path, String template) throws Exception {
        h5File = HdfFile.write(Paths.get(path));
        nxPathMapping.put(ROOT_PATH, h5File);

        var parser = new NxdlParser(template);
        parser.parse();
        var iterator = parser.iterator();
        while (iterator.hasNext()) {
            iterator.next().create(this);
        }
        open = System.currentTimeMillis();
        logger.info("Opened h5 file {} at {}", path, open);
    }

    private NxFile(WritableHdfFile h5File) {
        logger.warn("This overrides existing hdf structure");
        this.h5File = h5File;
        open = System.currentTimeMillis();
        logger.info("Opened h5 file {} at {}", h5File.getPath(), open);
    }

    //TODO this is basically broken, as the newly opened file overwrites all the structure from the template
    public static NxFile open(String path) {
        return new NxFile(HdfFile.write(Paths.get(path)));
    }

    public void write(String nxPath, Object v){
        var pgroup = (WritableGroup) nxPathMapping.get(nxPath.substring(0, nxPath.lastIndexOf("/")));
        if(pgroup == null) throw new IllegalArgumentException("No such nxPath: " + nxPath);
        var dataset = pgroup.putDataset(nxPath.substring(nxPath.lastIndexOf("/") + 1), v);
        nxPathMapping.putIfAbsent(nxPath, dataset);
    }

    public void append(String nxPath, Object newData) {
        var dataset = (WritableDataset) nxPathMapping.get(nxPath);
        if(dataset == null) write(nxPath, newData);
        else {
            var data = dataset.getData();
            var length1 = Array.getLength(data);
            var length2 = Array.getLength(newData);
            var result = Array.newInstance(data.getClass().getComponentType(), length1 + length2);
            System.arraycopy(data, 0, result, 0, length1);
            System.arraycopy(newData, 0, result, length1, length2);
            nxPathMapping.put(nxPath, ((WritableGroup)dataset.getParent()).putDataset(dataset.getName(), result));
        }
    }


    public void write(String nxPath, String v, boolean append) {
        if(append){
            append(nxPath, new String[]{v});
        } else {
            write(nxPath, new String[]{v});
        }


        logger.debug("Updated dataset: {}  with value: {}", nxPath, v);
    }

    public void write(String nxPath, double v, boolean append) {
        if(append){
            append(nxPath, new double[]{v});
        } else {
            write(nxPath, new double[]{v});
        }


        logger.debug("Updated dataset: {}  with value: {}", nxPath, v);

    }

    public void write(String nxPath, float v, boolean append) throws Exception {
        if(append){
            append(nxPath, new float[]{v});
        } else {
            write(nxPath, new float[]{v});
        }


        logger.debug("Updated dataset: {}  with value: {}", nxPath, v);

    }

    public void write(String nxPath, long v, boolean append) throws Exception {
        if(append){
            append(nxPath, new long[]{v});
        } else {
            write(nxPath, new long[]{v});
        }

        logger.debug("Updated dataset: {}  with value: {}", nxPath, v);

    }

    public void write(String nxPath, int v, boolean append) throws Exception {
        if(append){
            append(nxPath, new int[]{v});
        } else {
            write(nxPath, new int[]{v});
        }

        logger.debug("Updated dataset: {}  with value: {}", nxPath, v);

    }

    public void write(String nxPath, short v, boolean append) throws Exception {
        if(append){
            append(nxPath, new short[]{v});
        } else {
            write(nxPath, new short[]{v});
        }


        logger.debug("Updated dataset: {}  with value: {}", nxPath, v);

    }



    public void close() {
        h5File.close();
        long close = System.currentTimeMillis();
        long delta = close - open;
        logger.info("Closed h5 at {}. Total time {} ms", close, delta);
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
