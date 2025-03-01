package org.waltz.nexus;

import ncsa.hdf.object.Group;
import ncsa.hdf.object.h5.H5File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NxdlLink implements NxdlElement{
    private final Logger logger = LoggerFactory.getLogger("NxdlParser");

    private final String linkPath;
    private final String name;
    private final String targetPath;

    public NxdlLink(String linkPath, String name, String targetPath) {
        this.linkPath = linkPath;
        this.name = name;
        this.targetPath = targetPath;
    }

    @Override
    public void create(H5File h5file) throws Exception {
        var current = h5file.get(targetPath);
        if(current == null) throw new IllegalStateException("target path does not exist");
        var pgroup = h5file.get(linkPath);
        if(pgroup != null && !(pgroup instanceof Group)) throw new IllegalStateException("pgroup is not a group");
        h5file.createLink((Group) pgroup, name, current);
        logger.debug("Created Link {}: {} -> {}", name, linkPath, targetPath);
    }
}
