package hzg.wpn.tango;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.tango.client.ez.data.type.TangoImage;
import org.tango.client.ez.proxy.TangoProxies;
import org.tango.client.ez.proxy.TangoProxy;

import java.nio.file.Files;
import java.nio.file.Paths;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

/**
 * @author khokhria
 * @since 6/17/15.
 */
public class DataFormatServerTest {

    private DataFormatServer server = spy(new DataFormatServer());

    @Before
    public void before() throws Exception {
        doReturn(Thread.currentThread().getName()).when(server).getClientId();

        Files.createDirectories(Paths.get(server.getCwd()));
        Files.deleteIfExists(Paths.get(server.getCwd(), "test.h5"));
        server.createFile("test.h5");
    }

    @After
    public void after() throws Exception {
        server.closeFile();
    }

    @Test
    @Category(Integration.class)
    public void testWriteImage() throws Exception {
        TangoProxy mockCamera = TangoProxies.newDeviceProxyWrapper("tango://hzgctkit:10000/hzgctkit/imagedevice/0");

        TangoImage<short[]> image = mockCamera.readAttribute("Image");

        server.setNxPath("/entry/data/data");
        server.write16bitImage(image.getData());
    }
}