package hzg.wpn.tango;

import ncsa.hdf.hdf5lib.exceptions.HDF5Exception;
import ncsa.hdf.object.Dataset;
import ncsa.hdf.object.FileFormat;
import ncsa.hdf.object.h5.H5File;
import ncsa.hdf.object.h5.H5ScalarDS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.waltz.nexus.NxdlParser;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;

public class NxFile {
    private final Logger logger = LoggerFactory.getLogger(NxFile.class);

    private final H5File h5File;

    public NxFile(String path, String template) throws Exception {
        h5File = new H5File(path, FileFormat.CREATE);
        h5File.open();

        var parser = new NxdlParser(template);
        parser.parse();
        var iterator = parser.iterator();
        while (iterator.hasNext()) {
            iterator.next().create(h5File);
        }
    }

    private NxFile(H5File h5File) {
        this.h5File = h5File;
    }

    public static NxFile open(String path) {
        return new NxFile(new H5File(path, FileFormat.WRITE));
    }

    public void write(String nxPath, String v, boolean append) throws Exception {
        H5ScalarDS dataset = (H5ScalarDS) h5File.get(nxPath);
        if (dataset == null) throw new IllegalArgumentException(String.format("Dataset[%s] does not exist", nxPath));
        // Dataset exists, update it
        dataset.init();
        // Read existing values
        Object data = dataset.read();
        if (!(data instanceof String[]))
            throw new IllegalArgumentException(String.format("Dataset[%s] is not a double[]", nxPath));

        String[] casted = (String[]) data;

        String[] newData;
        if (append) {
            newData = new String[casted.length + 1];
            System.arraycopy(casted, 0, newData, 0, casted.length);
            newData[casted.length - 1] = v;
        } else {
            newData = new String[]{v};
        }

        update(dataset, newData, append);


        logger.debug("Updated dataset: {}  with value: {}", nxPath, v);

    }

    public void write(String nxPath, double v, boolean append) throws Exception {
        H5ScalarDS dataset = (H5ScalarDS) h5File.get(nxPath);
        if (dataset == null) throw new IllegalArgumentException(String.format("Dataset[%s] does not exist", nxPath));
        // Dataset exists, update it
        dataset.init();
        // Read existing values
        Object data = dataset.read();
        if (!(data instanceof double[]))
            throw new IllegalArgumentException(String.format("Dataset[%s] is not a double[]", nxPath));

        double[] casted = (double[]) data;

        double[] newData;
        if (append) {
            newData = new double[casted.length + 1];
            System.arraycopy(casted, 0, newData, 0, casted.length);
            newData[casted.length - 1] = v;
        } else {
            newData = new double[]{v};
        }

        update(dataset, newData, append);


        logger.debug("Updated dataset: {}  with value: {}", nxPath, v);

    }

    public void write(String nxPath, float v, boolean append) throws Exception {
        H5ScalarDS dataset = (H5ScalarDS) h5File.get(nxPath);
        if (dataset == null) throw new IllegalArgumentException(String.format("Dataset[%s] does not exist", nxPath));
        // Dataset exists, update it
        dataset.init();
        // Read existing values
        Object data = dataset.read();
        if (!(data instanceof float[]))
            throw new IllegalArgumentException(String.format("Dataset[%s] is not a double[]", nxPath));

        float[] casted = (float[]) data;

        float[] newData;
        if (append) {
            newData = new float[casted.length + 1];
            System.arraycopy(casted, 0, newData, 0, casted.length);
            newData[casted.length - 1] = v;
        } else {
            newData = new float[]{v};
        }

        update(dataset, newData, append);


        logger.debug("Updated dataset: {}  with value: {}", nxPath, v);

    }

    public void write(String nxPath, long v, boolean append) throws Exception {
        H5ScalarDS dataset = (H5ScalarDS) h5File.get(nxPath);
        if (dataset == null) throw new IllegalArgumentException(String.format("Dataset[%s] does not exist", nxPath));
        // Dataset exists, update it
        dataset.init();
        // Read existing values
        Object data = dataset.read();
        if (!(data instanceof long[]))
            throw new IllegalArgumentException(String.format("Dataset[%s] is not a double[]", nxPath));

        long[] casted = (long[]) data;

        long[] newData;
        if (append) {
            newData = new long[casted.length + 1];
            System.arraycopy(casted, 0, newData, 0, casted.length);
            newData[casted.length - 1] = v;
        } else {
            newData = new long[]{v};
        }

        update(dataset, newData, append);


        logger.debug("Updated dataset: {}  with value: {}", nxPath, v);

    }

    public void write(String nxPath, int v, boolean append) throws Exception {
        H5ScalarDS dataset = (H5ScalarDS) h5File.get(nxPath);
        if (dataset == null) throw new IllegalArgumentException(String.format("Dataset[%s] does not exist", nxPath));
        // Dataset exists, update it
        dataset.init();
        // Read existing values
        Object data = dataset.read();
        if (!(data instanceof int[]))
            throw new IllegalArgumentException(String.format("Dataset[%s] is not a double[]", nxPath));

        int[] casted = (int[]) data;

        int[] newData;
        if (append) {
            newData = new int[casted.length + 1];
            System.arraycopy(casted, 0, newData, 0, casted.length);
            newData[casted.length - 1] = v;
        } else {
            newData = new int[]{v};
        }

        update(dataset, newData, append);


        logger.debug("Updated dataset: {}  with value: {}", nxPath, v);

    }

    public void write(String nxPath, short v, boolean append) throws Exception {
        H5ScalarDS dataset = (H5ScalarDS) h5File.get(nxPath);
        if (dataset == null) throw new IllegalArgumentException(String.format("Dataset[%s] does not exist", nxPath));
        // Dataset exists, update it
        dataset.init();
        // Read existing values
        Object data = dataset.read();
        if (!(data instanceof short[]))
            throw new IllegalArgumentException(String.format("Dataset[%s] is not a double[]", nxPath));

        short[] casted = (short[]) data;

        short[] newData;
        if (append) {
            newData = new short[casted.length + 1];
            System.arraycopy(casted, 0, newData, 0, casted.length);
            newData[casted.length - 1] = v;
        } else {
            newData = new short[]{v};
        }

        update(dataset, newData, append);


        logger.debug("Updated dataset: {}  with value: {}", nxPath, v);

    }

    private void update(H5ScalarDS dataset, Object newData, boolean append) throws Exception {
        if (append) {
            // Resize dataset and write updated values
            long[] newDims = {Array.getLength(newData)};
            dataset.extend(newDims);
            dataset.write(newData);
        } else {
            dataset.write(newData);
        }
    }


    public void close() throws Exception {
        h5File.close();
    }

    public String getFileName() {
        return h5File.getFilePath();
    }

    public void write(String nxPath, double v) throws Exception {
        write(nxPath, v, false);
    }

    public void write(String nxPath, String v) throws Exception {
        write(nxPath, v, false);
    }
}
