package org.waltz.nexus;

import ncsa.hdf.object.Attribute;
import ncsa.hdf.object.Datatype;
import ncsa.hdf.object.Group;
import ncsa.hdf.object.h5.H5Datatype;
import ncsa.hdf.object.h5.H5File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NxdlGroup implements NxdlElement {
    private final Logger logger = LoggerFactory.getLogger("NxdlParser");

    private final String path;
    private final String name;
    private final String nxClass;

    public NxdlGroup(String path, String name, String nxClass) {
        if (nxClass == null || nxClass.isEmpty()) throw new IllegalArgumentException("nxClass can not be null or empty");
        this.path = path;
        this.name = name;
        this.nxClass = nxClass;
    }

    @Override
    public void create(H5File h5file) throws Exception {
        var pgroup = h5file.get(path);
        if(pgroup != null && !(pgroup instanceof Group)) throw new IllegalStateException("pgroup is not a group");
        Group group = h5file.createGroup(name, (Group) pgroup);
        h5file.writeAttribute(group,
                new Attribute("NX_class",
                    new H5Datatype(Datatype. CLASS_STRING, nxClass.length()+1, -1, -1),new long[]{1},new String[]{nxClass}),
                false);
        logger.debug("Created Group {}: {} [{}]", name ,path, nxClass);
    }
}
