package org.waltz.nexus;

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
    public void create(NxFile h5file) {
        logger.debug("Skipped creating Dataset {}: {} [{}]", name,  path, type);
    }
}
