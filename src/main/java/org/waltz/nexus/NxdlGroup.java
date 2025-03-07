package org.waltz.nexus;

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
    public void create(NxFile h5file)  {
        var parent = h5file.followThePath(path);
        var group = parent.putGroup(name);
        group.putAttribute("NX_class", nxClass);
        logger.debug("Created Group {}: {} [{}]", name ,path, nxClass);
    }
}
