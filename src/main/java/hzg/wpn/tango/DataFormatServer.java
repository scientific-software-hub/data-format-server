package hzg.wpn.tango;

import fr.esrf.Tango.ClntIdent;
import fr.esrf.Tango.JavaClntIdent;
import fr.esrf.Tango.LockerLanguage;
import hzg.wpn.nexus.libpniio.jni.NxFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tango.DeviceState;
import org.tango.server.ServerManager;
import org.tango.server.annotation.*;
import org.tango.server.device.DeviceManager;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author khokhria
 * @since 6/16/15.
 */
@Device
public class DataFormatServer {
    private static final Logger logger = LoggerFactory.getLogger(DataFormatServer.class);

    private static final Path XENV_ROOT = Paths.get(System.getProperty("XENV_ROOT") != null ? System.getProperty("XENV_ROOT") : "");

    private volatile Path nxTemplate = XENV_ROOT.resolve("etc/default.nxdl.xml");
    private volatile Path cwd = XENV_ROOT.resolve("var");
    private volatile NxFile nxFile;
    @Attribute
    private volatile String nxPath = "";
    @Attribute
    private volatile boolean append;

    @DeviceManagement
    private volatile DeviceManager deviceManager;
    private volatile String clientId;

    public static void main(String[] args) {
        ServerManager.getInstance().start(args, DataFormatServer.class);
    }

    public void setDeviceManager(DeviceManager manager) {
        deviceManager = manager;
    }

    public String getNxPath() {
        return nxPath;
    }

    public void setNxPath(String nxPath) throws Exception {
        clientId = getClientId();
        this.nxPath = nxPath;
    }

    public boolean getAppend() {
        return append;
    }

    public void setAppend(boolean v) {
        append = v;
    }

    @Attribute
    public String getClientId() throws Exception {
        ClntIdent clientIdentity = this.deviceManager.getClientIdentity();
        LockerLanguage discriminator = clientIdentity.discriminator();
        switch (discriminator.value()) {
            case LockerLanguage._JAVA:
                JavaClntIdent java_clnt = clientIdentity.java_clnt();
                return java_clnt.MainClass;
            case LockerLanguage._CPP:
                return "CPP " + clientIdentity.cpp_clnt();
        }
        throw new AssertionError("Should not happen");
    }


    @Attribute(isMemorized = true)
    public String getCwd() {
        return cwd.toAbsolutePath().toString();
    }

    @Attribute(isMemorized = true)
    public void setCwd(String cwd) {
        Path tmp = XENV_ROOT.resolve(cwd);
        if (!Files.isDirectory(tmp)) throw new IllegalArgumentException("Directory name is expected here!");
        this.cwd = tmp;
    }

    @Attribute(isMemorized = true)
    public String getNxTemplate() {
        return nxTemplate.toAbsolutePath().toString();
    }

    @Attribute(isMemorized = true)
    public void setNxTemplate(String nxTemplateName) {
        Path tmp = XENV_ROOT.resolve(nxTemplateName);
        if (!Files.exists(tmp)) throw new IllegalArgumentException("An existing .nxdl.xml file is expected here!");
        nxTemplate = tmp;
    }

    private boolean checkWritePermission(String clientId) throws Exception {
        return this.clientId.equals(clientId);
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

        if (!checkWritePermission(getClientId()))
            throw new IllegalStateException("write method call from a client must follow write to nx_path attribute by the same client.");

        logger.debug("Writing Integer: " + nxPath + " = " + v);
        nxFile.write(nxPath, v, append);
    }

    @Command
    public void writeLong(long v) throws Exception {
        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        if (!checkWritePermission(getClientId()))
            throw new IllegalStateException("write method call from a client must follow write to nx_path attribute by the same client.");

        logger.debug("Writing Long: " + nxPath + " = " + v);
        nxFile.write(nxPath, v, append);
    }

    @Command
    public void writeFloat(float v) throws Exception {
        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        if (!checkWritePermission(getClientId()))
            throw new IllegalStateException("write method call from a client must follow write to nx_path attribute by the same client.");

        logger.debug("Writing Float: " + nxPath + " = " + v);
        nxFile.write(nxPath, v, append);
    }

    @Command
    public void writeDouble(double v) throws Exception {
        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        if (!checkWritePermission(getClientId()))
            throw new IllegalStateException("write method call from a client must follow write to nx_path attribute by the same client.");

        logger.debug("Writing Double: " + nxPath + " = " + v);
        nxFile.write(nxPath, v, append);
    }

    @Command
    public void writeString(String v) throws Exception {
        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        if (!checkWritePermission(getClientId()))
            throw new IllegalStateException("write method call from a client must follow write to nx_path attribute by the same client.");

        logger.debug("Writing String: " + nxPath + " = " + v);
        nxFile.write(nxPath, v, append);
    }

    @Command
    public void write16bitImage(short[] data) throws Exception {
        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        if (!checkWritePermission(getClientId()))
            throw new IllegalStateException("write method call from a client must follow write to nx_path attribute by the same client.");

        logger.debug("Writing 16 bit image to " + nxPath);
        nxFile.write(nxPath, data, append);
    }

    @Command
    public void writeARGBImage(int[] data) throws Exception {
        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        if (!checkWritePermission(getClientId()))
            throw new IllegalStateException("write method call from a client must follow write to nx_path attribute by the same client.");

        logger.debug("Writing ARGB image to " + nxPath);
        nxFile.write(nxPath, data, append);
    }

    @Command
    public void writeTIFFImage(float[] data) throws Exception {
        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        if (!checkWritePermission(getClientId()))
            throw new IllegalStateException("write method call from a client must follow write to nx_path attribute by the same client.");

        logger.debug("Writing float image to " + nxPath);
        nxFile.write(nxPath, data, append);
    }

    @Init
    @StateMachine(endState = DeviceState.ON)
    public void init() throws Exception {
        Files.createDirectories(cwd);
    }

    @Delete
    @StateMachine(endState = DeviceState.OFF)
    public void delete() throws Exception {
        nxFile.close();
    }
}
