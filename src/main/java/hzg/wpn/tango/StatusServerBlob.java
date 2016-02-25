package hzg.wpn.tango;

import fr.esrf.Tango.DevFailed;
import fr.esrf.TangoApi.DevicePipe;
import fr.esrf.TangoApi.PipeBlob;
import fr.esrf.TangoApi.PipeDataElement;
import fr.esrf.TangoApi.PipeScanner;
import hzg.wpn.nexus.libpniio.jni.NxFile;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author Igor Khokhriakov <igor.khokhriakov@hzg.de>
 * @since 11.07.2015
 */
public class StatusServerBlob implements NexusWriter {
    public GenericBlob values = new GenericBlob(true);
    public GenericBlob times = new GenericBlob(true);

    public StatusServerBlob(PipeBlob blob) throws DevFailed {
        for (PipeDataElement dataElement : blob) {
            PipeBlob innerBlob = dataElement.extractPipeBlob();
            String nxPath = NexusWriterHelper.toNxPath(innerBlob.getName());
            PipeScanner innerBlobScanner = new DevicePipe(null, innerBlob);
            GenericBlob.Element elementValue = new GenericBlob.Element(nxPath + "/value", innerBlobScanner.nextArray());
            GenericBlob.Element elementTime = new GenericBlob.Element(nxPath + "/time", innerBlobScanner.nextArray(long[].class));

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
