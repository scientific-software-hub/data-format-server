package hzg.wpn.tango;

import hzg.wpn.nexus.libpniio.jni.NxFile;

import java.io.IOException;

/**
 * @author Igor Khokhriakov <igor.khokhriakov@hzg.de>
 * @since 13.07.2015
 */
public interface NexusWriter {
    void write(NxFile file) throws IOException;
}
