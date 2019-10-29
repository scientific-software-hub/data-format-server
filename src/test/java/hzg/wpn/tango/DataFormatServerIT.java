package hzg.wpn.tango;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.tango.client.ez.data.type.TangoImage;
import org.tango.client.ez.proxy.TangoProxies;
import org.tango.client.ez.proxy.TangoProxy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * @author khokhria
 * @since 6/17/15.
 */
public class DataFormatServerIT {

    private Path cwd;

    @Before
    public void before() throws Exception {
        cwd = Paths.get("target/var").toAbsolutePath();
        Files.deleteIfExists(Paths.get(cwd.toString(), "test.h5"));
    }


    //    @Test
    @Category(Integration.class)
    public void testWriteImage() throws Exception {
        TangoProxy mockCamera = TangoProxies.newDeviceProxyWrapper("tango://hzgctkit:10000/hzgctkit/imagedevice/0");

        TangoImage<short[]> image = mockCamera.readAttribute("Image");

//        server.setNxPath("/entry/data/data");
//        server.write16bitImage(image.getData());
    }

    @Test
    @Category(Integration.class)
    public void writeDoubleNan() throws Exception {
        TangoProxy dfs = TangoProxies.newDeviceProxyWrapper("tango://hzgxenvtest.desy.de:10000/development/dfs/0");

//        if(!dfs.readAttribute("cwd").equals(cwd.toString())){
//            dfs.writeAttribute("cwd", cwd.toString());
//        }

//        assertEquals(cwd.toString(), dfs.readAttribute("cwd"));

//        dfs.executeCommand("openFile", "/home/khokhria/Projects/jDFS/target/var/test.h5");

        dfs.executeCommand("createFile", "test.h5");


        dfs.writeAttribute("nxPath", "/entry/instrument/detector/distance");
        dfs.executeCommand("writeDouble", Double.NaN);


        dfs.executeCommand("closeFile", null);
    }

    @Test
    @Category(Integration.class)
    public void writeManyDoubles() throws Exception {
        TangoProxy dfs = TangoProxies.newDeviceProxyWrapper("tango://hzgxenvtest.desy.de:10000/development/dfs/0");

//        if(!dfs.readAttribute("cwd").equals(cwd.toString())){
//            dfs.writeAttribute("cwd", cwd.toString());
//        }

//        assertEquals(cwd.toString(), dfs.readAttribute("cwd"));

//        dfs.executeCommand("openFile", "/home/khokhria/Projects/jDFS/target/var/test.h5");

        dfs.executeCommand("createFile", "test_writeManyDoubles.h5");


        dfs.writeAttribute("append", true);
        dfs.writeAttribute("nxPath", "/entry/instrument/detector/distance");

        //takes 2,2 s
        for (int i = 0; i < 1000; ++i) {
            dfs.executeCommand("writeDouble", Math.random());
        }

        dfs.executeCommand("closeFile", null);
    }

    @Test
    @Category(Integration.class)
    public void writeDoubleArray() throws Exception {
        TangoProxy dfs = TangoProxies.newDeviceProxyWrapper("tango://hzgxenvtest.desy.de:10000/development/dfs/0");

        dfs.executeCommand("openFile", "test_writeDoubleArray.h5");

//        dfs.executeCommand("createFile", "test_writeDoubleArray.h5");

        dfs.writeAttribute("nxPath", "/entry/instrument/detector/distance");

        double[] doubles = new double[1000];
        Arrays.setAll(doubles, p -> Math.random());
        dfs.executeCommand("writeDoubleArray", doubles);

        dfs.executeCommand("closeFile", null);
    }

    @Test
    @Category(Integration.class)
    public void writeStringArray() throws Exception {
        TangoProxy dfs = TangoProxies.newDeviceProxyWrapper("tango://hzgxenvtest.desy.de:10000/development/dfs/0");

//        dfs.executeCommand("openFile", "/home/khokhria/Projects/jDFS/target/var/test.h5");

        dfs.executeCommand("createFile", "test_writeStringArray.h5");

        dfs.writeAttribute("nxPath", "/entry/start_time");

        String[] strings = new String[1000];
        Arrays.fill(strings, new String("Ah-oh"));
        dfs.executeCommand("writeStringArray", strings);

        dfs.executeCommand("closeFile", null);
    }

    @Test
    @Category(Integration.class)
    public void writeLongArray() throws Exception {
        TangoProxy dfs = TangoProxies.newDeviceProxyWrapper("tango://hzgxenvtest.desy.de:10000/development/dfs/0");

//        dfs.executeCommand("openFile", "/home/khokhria/Projects/jDFS/target/var/test.h5");

        dfs.executeCommand("createFile", "test_writeLongArray.h5");

        dfs.writeAttribute("nxPath", "/entry/end_time");

        long[] longs = new long[1000];
        Arrays.setAll(longs, p -> (long) (Math.random() * 1000));
        dfs.executeCommand("writeLongArray", longs);

        dfs.executeCommand("closeFile", null);
    }
}