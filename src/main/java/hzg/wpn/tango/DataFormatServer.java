package hzg.wpn.tango;

import hzg.wpn.nexus.libpniio.jni.NxFile;
import org.tango.server.ServerManager;
import org.tango.server.annotation.Attribute;
import org.tango.server.annotation.Command;
import org.tango.server.annotation.Device;

import java.io.IOException;
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
    private volatile Path cwd = XENV_ROOT.resolve("var");
    private volatile NxFile nxFile;
    @Attribute
    private volatile String nxPath = "";
    @Attribute
    private volatile boolean append;

    //TODO move intializations to init
    {
        try {
            Files.createDirectories(cwd);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

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
    public String getCwd() {
        return cwd.toAbsolutePath().toString();
    }

    @Attribute
    public void setCwd(String cwd) {
        Path tmp = Paths.get(cwd);
        if (!Files.isDirectory(tmp)) throw new IllegalArgumentException("Directory name is expected here!");
        this.cwd = tmp;
    }

    @Attribute
    public String getNxTemplate() {
        return nxTemplate.toAbsolutePath().toString();
    }

    @Attribute
    public void setNxTemplate(String nxTemplateName) {
        Path tmp = Paths.get(nxTemplateName);
        if (!Files.exists(tmp)) throw new IllegalArgumentException("An existing .nxdl.xml file is expected here!");
        nxTemplate = tmp;
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
        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");
        nxFile.write(nxPath, v, append);
    }

    @Command
    public void writeLong(long v) throws Exception {
        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");
        nxFile.write(nxPath, v, append);
    }

    @Command
    public void writeFloat(float v) throws Exception {
        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");
        nxFile.write(nxPath, v, append);
    }

    @Command
    public void writeDouble(double v) throws Exception {
        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");
        nxFile.write(nxPath, v, append);
    }

    @Command
    public void writeString(String v) throws Exception {
        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");
        nxFile.write(nxPath, v, append);
    }

    @Command
    public void write16bitImage(short[] data) throws Exception {
        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");
        nxFile.write(nxPath, data, append);
    }

    @Command
    public void writeARGBImage(int[] data) throws Exception {
        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");
        nxFile.write(nxPath, data, append);
    }

    @Command
    public void writeTIFFImage(float[] data) throws Exception {
        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");
        nxFile.write(nxPath, data, append);
    }
}
