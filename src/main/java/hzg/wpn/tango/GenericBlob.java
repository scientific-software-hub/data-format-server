package hzg.wpn.tango;

import fr.esrf.Tango.DevFailed;
import fr.esrf.TangoApi.DevicePipe;
import fr.esrf.TangoApi.PipeBlob;
import fr.esrf.TangoApi.PipeDataElement;
import fr.esrf.TangoApi.PipeScanner;
import hzg.wpn.nexus.libpniio.jni.LibpniioException;
import hzg.wpn.nexus.libpniio.jni.NxFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Contains an array of values associated with an nxPath
 *
 * @author Igor Khokhriakov <igor.khokhriakov@hzg.de>
 * @since 11.07.2015
 */
public class GenericBlob implements NexusWriter {
    private final Logger logger = LoggerFactory.getLogger(GenericBlob.class);

    public final List<Element> elements = new ArrayList<>();
    private final boolean append;


    public GenericBlob(boolean append) {
        this.append = append;
    }

    public GenericBlob(PipeBlob blob, boolean append) throws DevFailed {
        this.append = append;
        for (PipeDataElement dataElement : blob) {
            PipeBlob innerBlob = dataElement.extractPipeBlob();
            PipeScanner scanner = new DevicePipe(null, innerBlob);
            Element element = new Element(
                    NexusWriterHelper.toNxPath(innerBlob.getName()), scanner.nextArray());
            elements.add(element);
        }
    }

    @Override
    public void write(NxFile file) {
        for (Element element : elements) {
            Class<?> aClass = element.value.getClass().getComponentType();
            for (int i = 0, size = Array.getLength(element.value); i < size; ++i) {
                try {
                    logger.debug("{} data to nexus file: {}={}", append ? "Appending" : "Writing", element.nxPath, String.valueOf(Array.get(element.value, i)));
                    if (aClass == Integer.class || aClass == Short.class ||
                            aClass == Byte.class || aClass == int.class || aClass == short.class ||
                            aClass == byte.class) {
                        file.write(element.nxPath, Array.getInt(element.value, i), append);
                    } else if (aClass == Long.class || aClass == long.class) {
                        file.write(element.nxPath, Array.getLong(element.value, i), append);
                    } else if (aClass == Float.class || aClass == float.class) {
                        file.write(element.nxPath, Array.getFloat(element.value, i), append);
                    } else if (aClass == Double.class || aClass == double.class) {
                        file.write(element.nxPath, Array.getDouble(element.value, i), append);
                    } else {
                        file.write(element.nxPath, String.valueOf(Array.get(element.value, i)), append);
                    }
                } catch (LibpniioException e) {
                    logger.warn("Write to NeXus file has failed: {}", e.getMessage());
                    continue;
                }
            }
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
