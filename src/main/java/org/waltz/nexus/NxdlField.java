package org.waltz.nexus;

import ncsa.hdf.object.Datatype;
import ncsa.hdf.object.Group;
import ncsa.hdf.object.h5.H5Datatype;
import ncsa.hdf.object.h5.H5File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NxdlField implements NxdlElement{
    private final Logger logger = LoggerFactory.getLogger("NxdlParser");
    private final String path;
    private final String name;
    private final String type;
    private final long[] dimensions;

    public NxdlField(String path, String name, String type, long[] dimensions) {
        this.path = path;
        this.name = name;
        this.type = type;
        this.dimensions = dimensions;
    }

    @Override
    public void create(H5File h5file) throws Exception {
        var pgroup = h5file.get(path);
        if(pgroup != null && !(pgroup instanceof Group)) throw new IllegalStateException("pgroup is not a group");
        H5Datatype dtype = getH5Datatype(type);
        h5file.createScalarDS(name, (Group) pgroup, dtype, dimensions, null, null, 0, null);
        logger.debug("Created Dataset {}: {} [{}]", name,  path, type);
    }

    private H5Datatype getH5Datatype(String type){
        switch (type) {
            case "int64":
            case "uint64":
                return new H5Datatype(Datatype.CLASS_INTEGER, 8, Datatype.ORDER_LE, Datatype.SIGN_NONE);
            case "int32":
            case "uint32":
                return new H5Datatype(Datatype.CLASS_INTEGER, 4, Datatype.ORDER_LE, Datatype.SIGN_NONE);
            case "int16":
                return new H5Datatype(Datatype.CLASS_INTEGER, 2, Datatype.ORDER_LE, Datatype.SIGN_NONE);
            case "float64":
                return new H5Datatype(Datatype.CLASS_FLOAT, 8, Datatype.ORDER_LE, Datatype.SIGN_NONE);
            case "float32":
                return new H5Datatype(Datatype.CLASS_FLOAT, 4, Datatype.ORDER_LE, Datatype.SIGN_NONE);
            case "string":
                return new H5Datatype(Datatype.CLASS_STRING, -1, Datatype.ORDER_LE, Datatype.SIGN_NONE);
            default:
                throw new IllegalArgumentException("Unsupported data type: " + type);
        }
    }
}
