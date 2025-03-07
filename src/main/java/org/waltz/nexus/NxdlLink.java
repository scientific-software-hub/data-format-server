package org.waltz.nexus;

import io.jhdf.WritableHdfFile;
import ncsa.hdf.object.Group;
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
    public void create(NxFile h5file) {
        logger.warn("Unsupported feature: created Link {}: {} -> {}", name, linkPath, targetPath);
    }
}
