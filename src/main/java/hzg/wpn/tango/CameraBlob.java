package hzg.wpn.tango;

import fr.esrf.Tango.DevFailed;
import fr.esrf.TangoApi.DevicePipe;
import fr.esrf.TangoApi.PipeBlob;
import fr.esrf.TangoApi.PipeScanner;

/**
 * Designed to be thread confinement
 *
 * @author Igor Khokhriakov <igor.khokhriakov@hzg.de>
 * @since 11.07.2015
 */
public class CameraBlob {
    public String nxPath;
    public ImageType type;
    public byte[] image;


    public CameraBlob(PipeBlob blob) throws DevFailed {
        PipeScanner scanner = new DevicePipe(null, blob);
        nxPath = scanner.nextString();
        type = ImageType.valueOf(scanner.nextString());
        image = scanner.nextArray(byte[].class);
    }

    public static enum ImageType {
        _16BIT,
        ARGB,
        TIFF
    }
}
