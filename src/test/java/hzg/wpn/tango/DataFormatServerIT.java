package hzg.wpn.tango;

import fr.esrf.Tango.DevState;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.tango.client.ez.data.type.TangoImage;
import org.tango.client.ez.proxy.TangoProxies;
import org.tango.client.ez.proxy.TangoProxy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import static junit.framework.TestCase.assertSame;

/**
 * @author khokhria
 * @since 6/17/15.
 */
public class DataFormatServerIT {
    public static final String TEST_DEVICE = "tango://localhost:10000/dev/xenv/dfs";

    private Path cwd;
    private String testName;

    @Rule
    public TestRule watcher = new TestWatcher() {
        protected void starting(Description description) {
            testName = description.getMethodName();
        }
    };

    private String getTestFileOutputName(){
        return "target/test_"+testName+".h5";
    }

    @Before
    public void before() throws Exception {
        Files.deleteIfExists(Paths.get(getTestFileOutputName()));
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
        TangoProxy dfs = TangoProxies.newDeviceProxyWrapper(TEST_DEVICE);

//        if(!dfs.readAttribute("cwd").equals(cwd.toString())){
//            dfs.writeAttribute("cwd", cwd.toString());
//        }

//        assertEquals(cwd.toString(), dfs.readAttribute("cwd"));

//        dfs.executeCommand("openFile", "/home/khokhria/Projects/jDFS/target/var/test.h5");

        dfs.executeCommand("createFile", getTestFileOutputName());


        dfs.writeAttribute("nxPath", "/entry/instrument/detector/distance");
        dfs.executeCommand("writeDouble", Double.NaN);


        dfs.executeCommand("closeFile", null);
    }

    @Test
    @Category(Integration.class)
    public void writeString() throws Exception {
        TangoProxy dfs = TangoProxies.newDeviceProxyWrapper(TEST_DEVICE);

//        if(!dfs.readAttribute("cwd").equals(cwd.toString())){
//            dfs.writeAttribute("cwd", cwd.toString());
//        }

//        assertEquals(cwd.toString(), dfs.readAttribute("cwd"));

//        dfs.executeCommand("openFile", "/home/khokhria/Projects/jDFS/target/var/test.h5");

        dfs.executeCommand("createFile", getTestFileOutputName());


        dfs.writeAttribute("nxPath", "/entry/definition");
        dfs.executeCommand("writeString", "Hello World!!!");


        dfs.executeCommand("closeFile", null);
    }

    @Test
    @Category(Integration.class)
    public void appendString() throws Exception {
        TangoProxy dfs = TangoProxies.newDeviceProxyWrapper(TEST_DEVICE);

//        if(!dfs.readAttribute("cwd").equals(cwd.toString())){
//            dfs.writeAttribute("cwd", cwd.toString());
//        }

//        assertEquals(cwd.toString(), dfs.readAttribute("cwd"));

//        dfs.executeCommand("openFile", "/home/khokhria/Projects/jDFS/target/var/test.h5");

        dfs.executeCommand("createFile", getTestFileOutputName());


        dfs.writeAttribute("nxPath", "/entry/instrument/detector/file");
        dfs.executeCommand("appendString", "1");
        dfs.executeCommand("appendString", "2");
        dfs.executeCommand("appendString", "3");
        dfs.executeCommand("appendString", "Ka-boom!!!");


        dfs.executeCommand("closeFile", null);
    }

    @Test
    @Category(Integration.class)
    public void writeManyDoubles() throws Exception {
        TangoProxy dfs = TangoProxies.newDeviceProxyWrapper(TEST_DEVICE);

//        if(!dfs.readAttribute("cwd").equals(cwd.toString())){
//            dfs.writeAttribute("cwd", cwd.toString());
//        }

//        assertEquals(cwd.toString(), dfs.readAttribute("cwd"));

//        dfs.executeCommand("openFile", "/home/khokhria/Projects/jDFS/target/var/test.h5");

        dfs.executeCommand("createFile", getTestFileOutputName());


        dfs.writeAttribute("nxPath", "/entry/instrument/detector/distance");

        //takes 2,2 s
        for (int i = 0; i < 1000; ++i) {
            dfs.executeCommand("appendDouble", Math.random());
        }

        dfs.executeCommand("closeFile", null);
    }

    @Test
    @Category(Integration.class)
    public void writeDoubleArray() throws Exception {
        TangoProxy dfs = TangoProxies.newDeviceProxyWrapper(TEST_DEVICE);

        dfs.executeCommand("createFile", getTestFileOutputName());

//        dfs.executeCommand("createFile", "test_writeDoubleArray.h5");

        dfs.writeAttribute("nxPath", "/entry/instrument/detector/distance");

        double[] doubles = new double[100_000];
        Arrays.setAll(doubles, p -> Math.random());
        dfs.executeCommand("writeDoubleArray", doubles);

        dfs.executeCommand("closeFile", null);
    }

    @Test
    @Category(Integration.class)
    public void writeDoubleArrayWithFlush() throws Exception {
        TangoProxy dfs = TangoProxies.newDeviceProxyWrapper(TEST_DEVICE);

        dfs.executeCommand("createFile", getTestFileOutputName());

//        dfs.executeCommand("createFile", "test_writeDoubleArray.h5");

        dfs.writeAttribute("nxPath", "/entry/instrument/detector/distance");

        double[] doubles = new double[500];
        Arrays.setAll(doubles, p -> Math.random());
        dfs.executeCommand("writeDoubleArray", doubles);
        dfs.executeCommand("flush", null);

        Arrays.setAll(doubles, p -> Math.random());
        dfs.executeCommand("writeDoubleArray", doubles);
        dfs.executeCommand("closeFile", null);
    }

    @Test
    @Category(Integration.class)
    public void writeStringArray() throws Exception {
        TangoProxy dfs = TangoProxies.newDeviceProxyWrapper(TEST_DEVICE);

//        dfs.executeCommand("openFile", "/home/khokhria/Projects/jDFS/target/var/test.h5");

        dfs.executeCommand("createFile", getTestFileOutputName());

        dfs.writeAttribute("nxPath", "/entry/instrument/detector/file");

        String[] strings = new String[1000];
        Arrays.fill(strings, new String("Ah-oh"));
        dfs.executeCommand("writeStringArray", strings);

        dfs.executeCommand("closeFile", null);
    }

    @Test
    @Category(Integration.class)
    public void writeLongArray() throws Exception {
        TangoProxy dfs = TangoProxies.newDeviceProxyWrapper(TEST_DEVICE);

//        dfs.executeCommand("openFile", "/home/khokhria/Projects/jDFS/target/var/test.h5");

        dfs.executeCommand("createFile", getTestFileOutputName());

        dfs.writeAttribute("nxPath", "/entry/end_time");

        long[] longs = new long[1000];
        Arrays.setAll(longs, p -> (long) (Math.random() * 1000));
        dfs.executeCommand("writeLongArray", longs);

        dfs.executeCommand("closeFile", null);
    }

    @Test
    @Category(Integration.class)
    public void createNonExisting() throws Exception {
        TangoProxy dfs = TangoProxies.newDeviceProxyWrapper(TEST_DEVICE);

//        dfs.executeCommand("openFile", "/home/khokhria/Projects/jDFS/target/var/test.h5");

        dfs.executeCommand("createFile", "/tmp/x/y/z.h5");

        dfs.executeCommand("closeFile", null);

        DevState state = dfs.readAttribute("State");

        assertSame(DevState.FAULT, state);
    }
}