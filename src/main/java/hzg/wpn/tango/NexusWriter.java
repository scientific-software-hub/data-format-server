package hzg.wpn.tango;

import org.waltz.nexus.NxFile;

/**
 * @author Igor Khokhriakov <igor.khokhriakov@hzg.de>
 * @since 13.07.2015
 */
public abstract class NexusWriter {
    final String nxPath;

    public NexusWriter(String nxPath) {
        this.nxPath = nxPath;
    }

    abstract void write(NxFile file) throws Exception;
}
