package hzg.wpn.tango;

import fr.esrf.Tango.DevFailed;
import fr.esrf.TangoApi.DevicePipe;
import fr.esrf.TangoApi.PipeBlob;
import fr.esrf.TangoApi.PipeScanner;
import hzg.wpn.nexus.libpniio.jni.NxFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

/**
 * Contains an array of values associated with an nxPath
 *
 * @author Igor Khokhriakov <igor.khokhriakov@hzg.de>
 * @since 11.07.2015
 */
public class GenericBlob implements NexusWriter {
    private static final Logger logger = LoggerFactory.getLogger(GenericBlob.class);

    public final List<Element> elements = new ArrayList<>();
    private final boolean append;


    public GenericBlob(boolean append) {
        this.append = append;
    }

    public GenericBlob(PipeBlob blob, boolean append) throws DevFailed {
        this.append = append;
        PipeScanner scanner = new DevicePipe(null, blob);
        while (scanner.hasNext()) {
            PipeScanner innerScanner = scanner.nextScanner();
            Element element = new Element(innerScanner.nextString(), innerScanner.nextArray());
            elements.add(element);
        }
    }

    @Override
    public void write(NxFile file) throws IOException {
        try {
            for (Element e : elements) {
                Class<?> aClass = e.value.getClass().getComponentType();
                for (int i = 0, size = Array.getLength(e.value); i < size; ++i) {
                    logger.debug("Writing data to nexus file: " + e.nxPath + "=" + Array.get(e.value, i));
                    if (aClass == Integer.class || aClass == Short.class ||
                            aClass == Byte.class || aClass == int.class || aClass == short.class ||
                            aClass == byte.class) {
                        file.write(e.nxPath, Array.getInt(e.value, i), append);
                    } else if (aClass == Long.class || aClass == long.class) {
                        file.write(e.nxPath, Array.getLong(e.value, i), append);
                    } else if (aClass == Float.class || aClass == float.class) {
                        file.write(e.nxPath, Array.getFloat(e.value, i), append);
                    } else if (aClass == Double.class || aClass == double.class) {
                        file.write(e.nxPath, Array.getDouble(e.value, i), append);
                    } else {
                        file.write(e.nxPath, String.valueOf(Array.get(e.value, i)), append);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Write to NeXus file has failed!", e);
            throw new IOException(e);
        }
    }

    public static class Element {
        public String nxPath;
        public Object value;

        public Element(String nxPath, Object value) {
            this.nxPath = nxPath;
            this.value = value;
        }
    }
}
