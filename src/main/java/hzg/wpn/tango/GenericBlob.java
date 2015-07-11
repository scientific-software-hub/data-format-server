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
public class GenericBlob {
    public List<Element> elements = new ArrayList<>();

    public GenericBlob(PipeBlob blob) throws DevFailed {
        PipeScanner scanner = new DevicePipe(null, blob);
        while (scanner.hasNext()) {
            PipeScanner innerScanner = scanner.nextScanner();
            Element element = new Element();
            element.nxPath = innerScanner.nextString();
            element.data = innerScanner.something();
            elements.add(element);
        }
    }

    public static class Element {
        public String nxPath;
        public Object data;
    }
}
