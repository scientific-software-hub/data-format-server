package hzg.wpn.tango;

import hzg.wpn.nexus.libpniio.jni.NxFile;
import org.tango.server.ServerManager;
import org.tango.server.annotation.Attribute;
import org.tango.server.annotation.Command;
import org.tango.server.annotation.Device;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author khokhria
 * @since 6/16/15.
 */
@Device
public class DataFormatServer {
    private static final Path XENV_ROOT = Paths.get(System.getProperty("XENV_ROOT") != null ? System.getProperty("XENV_ROOT") : "");

    private volatile Path nxTemplate = XENV_ROOT.resolve("etc/default.nxdl.xml");
    private volatile Path cwd = XENV_ROOT.resolve("var/DataFormatServer");
    private volatile NxFile nxFile;

    @Attribute
    private volatile String nxPath;
    @Attribute
    private volatile boolean append;

    public static void main(String[] args) {
        ServerManager.getInstance().start(args, DataFormatServer.class);
    }

    public String getNxPath() {
        return nxPath;
    }

    public void setNxPath(String nxPath) {
        this.nxPath = nxPath;
    }

    public boolean getAppend() {
        return append;
    }

    public void setAppend(boolean v) {
        append = v;
    }

    @Attribute
    public void setCwd(String cwd) {
        Path tmp = Paths.get(cwd);
        if (!Files.isDirectory(tmp)) throw new IllegalArgumentException("Directory name is expected here!");
        this.cwd = tmp;
    }

    @Attribute
    public void setNxTemplate(String nxTemplateName) {
        Path tmp = Paths.get(nxTemplateName);
        if (!Files.exists(tmp)) throw new IllegalArgumentException("An existing .nxdl.xml file is expected here!");
        nxTemplate = tmp;
    }

    @Attribute
    public void setUShortImage(int[][] image) throws Exception {
        int height = image.length;
        int width = image[0].length;

        int[] data = new int[height * width];

        for (int i = 0; i < height; ++i) {
            System.arraycopy(image[i], 0, data, i * width, width);
        }

        nxFile.write(nxPath, data);
    }

    @Attribute
    public void setFloatImage(float[][] image) throws Exception {
        int height = image.length;
        int width = image[0].length;

        float[] data = new float[height * width];

        for (int i = 0; i < height; ++i) {
            System.arraycopy(image[i], 0, data, i * width, width);
        }

        nxFile.write(nxPath, data);
    }

    @Command
    public void createFile(String fileName) throws Exception {
        nxFile = NxFile.create(cwd.resolve(fileName).toAbsolutePath().toString(), nxTemplate.toAbsolutePath().toString());
    }

    @Command
    public void openFile(String fileName) throws Exception {
        if (nxFile != null) nxFile.close();

        nxFile = NxFile.open(cwd.resolve(fileName).toAbsolutePath().toString());
    }

    @Command
    public void closeFile() throws Exception {
        nxFile.close();
        nxFile = null;
    }

    @Command
    public void writeInteger(int v) throws Exception {
        nxFile.write(nxPath, v);
    }

    @Command
    public void writeLong(long v) throws Exception {
        nxFile.write(nxPath, v);
    }

    @Command
    public void writeFloat(float v) throws Exception {
        nxFile.write(nxPath, v);
    }

    @Command
    public void writeDouble(double v) throws Exception {
        nxFile.write(nxPath, v);
    }

    @Command
    public void writeString(String v) throws Exception {
        nxFile.write(nxPath, v);
    }
}
