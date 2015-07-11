package hzg.wpn.tango;

import fr.esrf.Tango.DevFailed;
import fr.esrf.TangoApi.DevicePipe;
import fr.esrf.TangoApi.PipeBlob;
import fr.esrf.TangoApi.PipeScanner;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Igor Khokhriakov <igor.khokhriakov@hzg.de>
 * @since 11.07.2015
 */
public class StatusServerBlob {
    public List<Element> elements = new ArrayList<>();

    public StatusServerBlob(PipeBlob blob) throws DevFailed {
        PipeScanner scanner = new DevicePipe(null, blob);
        while (scanner.hasNext()) {
            PipeScanner innerBlobScanner = scanner.nextScanner();
            Element element = new Element();
            element.nxPath = innerBlobScanner.nextString();
            element.values = innerBlobScanner.nextArray();
            element.times = innerBlobScanner.nextArray(long[].class);
            elements.add(element);
        }

    }

    public static class Element {
        public String nxPath;
        public Object values;//array
        public long[] times;
    }
}
