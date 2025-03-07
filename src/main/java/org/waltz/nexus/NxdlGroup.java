package org.waltz.nexus;

import io.jhdf.api.WritableGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.waltz.nexus.NxdlParser.ROOT_PATH;

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
        var parent = (WritableGroup)h5file.nxPathMapping.get(path);
        if(parent == null) throw new IllegalStateException();
        var group = parent.putGroup(name);
        group.putAttribute("NX_class", nxClass);
        h5file.nxPathMapping.put(parent == h5file.getH5File() ? ROOT_PATH + name  : path + "/" + name, group);
        logger.debug("Created Group {}: {} [{}]", name ,path, nxClass);
    }
}
