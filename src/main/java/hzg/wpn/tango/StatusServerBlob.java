package hzg.wpn.tango;

import fr.esrf.Tango.DevFailed;
import fr.esrf.TangoApi.DevicePipe;
import fr.esrf.TangoApi.PipeBlob;
import fr.esrf.TangoApi.PipeScanner;
import hzg.wpn.nexus.libpniio.jni.NxFile;

import java.io.IOException;

/**
 * @author Igor Khokhriakov <igor.khokhriakov@hzg.de>
 * @since 11.07.2015
 */
public class StatusServerBlob implements NexusWriter {
    public GenericBlob values = new GenericBlob();
    public GenericBlob times = new GenericBlob();

    public StatusServerBlob(PipeBlob blob) throws DevFailed {
        PipeScanner scanner = new DevicePipe(null, blob);
        while (scanner.hasNext()) {
            PipeScanner innerBlobScanner = scanner.nextScanner();
            GenericBlob.Element elementValue = new GenericBlob.Element();
            String nxPath = innerBlobScanner.nextString();
            elementValue.nxPath = nxPath + "/value";
            elementValue.value = innerBlobScanner.nextArray();

            GenericBlob.Element elementTime = new GenericBlob.Element();
            elementValue.nxPath = nxPath + "/time";
            elementTime.value = innerBlobScanner.nextArray(long[].class);

            values.elements.add(elementValue);
            times.elements.add(elementTime);
        }

    }

    @Override
    public void write(NxFile file) throws IOException {
        values.write(file);
        times.write(file);
    }
}
