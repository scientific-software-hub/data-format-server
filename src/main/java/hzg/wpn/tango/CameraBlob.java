package hzg.wpn.tango;

import fr.esrf.Tango.DevFailed;
import fr.esrf.TangoApi.DevicePipe;
import fr.esrf.TangoApi.PipeBlob;
import fr.esrf.TangoApi.PipeScanner;
import hzg.wpn.nexus.libpniio.jni.LibpniioException;
import hzg.wpn.nexus.libpniio.jni.NxFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Designed to be thread confinement
 *
 * @author Igor Khokhriakov <igor.khokhriakov@hzg.de>
 * @since 11.07.2015
 */
public class CameraBlob implements NexusWriter {
    private static final Logger logger = LoggerFactory.getLogger(CameraBlob.class);

    public boolean append = true;
    public String nxPath;
    public ImageType type;
    public Object image;


    public CameraBlob(PipeBlob blob, boolean append) throws DevFailed {
        PipeScanner scanner = new DevicePipe(null, blob);
        nxPath = scanner.nextString();
        type = ImageType.valueOf(scanner.nextString());
        image = scanner.nextArray();
        this.append = append;
    }

    @Override
    public void write(NxFile file) throws IOException {
        try {
            switch (type) {
                case _16BIT:
                    file.write(nxPath, (short[]) image, append);
                    break;
                case ARGB:
                    file.write(nxPath, (int[]) image, append);
                    break;
                case TIFF:
                    file.write(nxPath, (float[]) image, append);
                    break;
            }
        } catch (LibpniioException e) {
            logger.error("Image write into nexus file has failed!", e);
            throw new IOException(e);
        }
    }

    public static enum ImageType {
        _16BIT,
        ARGB,
        TIFF
    }
}
