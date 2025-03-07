package hzg.wpn.tango;

import fr.esrf.Tango.DevFailed;
import fr.esrf.TangoApi.DevicePipe;
import fr.esrf.TangoApi.PipeBlob;
import fr.esrf.TangoApi.PipeDataElement;
import fr.esrf.TangoApi.PipeScanner;
import org.waltz.nexus.NxFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

/**
 * Contains an array of values associated with an nxPath
 *
 * @author Igor Khokhriakov <igor.khokhriakov@hzg.de>
 * @since 11.07.2015
 */
public class GenericBlob extends NexusWriter {
    private final Logger logger = LoggerFactory.getLogger(GenericBlob.class);

    public final List<Element> elements = new ArrayList<>();
    private final boolean append;


    public GenericBlob(boolean append) {
        super(null);
        this.append = append;
    }

    public GenericBlob(PipeBlob blob) throws DevFailed {
        super(null);
        this.append = blob.get(0).extractBooleanArray()[0];


        for (PipeDataElement dataElement : blob.get(1).extractPipeBlob()) {
            PipeBlob innerBlob = dataElement.extractPipeBlob();
            PipeScanner scanner = new DevicePipe(null, innerBlob);
            String nxPath = innerBlob.getName();
            if(NexusWriterHelper.hasMapping(nxPath)) {
                nxPath = NexusWriterHelper.toNxPath(nxPath);
            }

            Element element = new Element(
                    nxPath, scanner.nextArray());
            elements.add(element);
        }
    }

    @Override
    public void write(NxFile file) {
        for (Element element : elements) {
            Class<?> aClass = element.value.getClass().getComponentType();

            if(append)
                file.append(element.nxPath, element.value);
            else
                file.write(element.nxPath, element.value);
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
