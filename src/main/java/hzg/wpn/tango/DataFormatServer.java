package hzg.wpn.tango;

import com.google.common.base.Preconditions;
import fr.esrf.Tango.DevFailed;
import fr.esrf.TangoApi.PipeBlobBuilder;
import hzg.wpn.nexus.libpniio.jni.LibpniioException;
import hzg.wpn.nexus.libpniio.jni.NxFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tango.DeviceState;
import org.tango.server.InvocationContext;
import org.tango.server.ServerManager;
import org.tango.server.annotation.*;
import org.tango.server.device.DeviceManager;
import org.tango.server.pipe.PipeValue;
import org.tango.utils.ClientIDUtil;
import org.tango.utils.DevFailedUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.*;

/**
 * @author khokhria
 * @since 6/16/15.
 */
@Device(transactionType = TransactionType.NONE)
public class DataFormatServer {
    private static final Path XENV_ROOT;

    static {
        String xenv_root;

        XENV_ROOT = Paths.get(
                (xenv_root = System.getProperty("XENV_ROOT", System.getenv("XENV_ROOT"))) == null ? "" : xenv_root);


    }

    private final Logger logger = LoggerFactory.getLogger(DataFormatServer.class);
    private final ExecutorService exec = Executors.newSingleThreadExecutor();
    //clientId -> nxPath
    private final ConcurrentMap<String, String> clientNxPath = new ConcurrentHashMap<>();
    private volatile Path nxTemplate = XENV_ROOT.resolve("etc/default.nxdl.xml");
    private volatile Path cwd = XENV_ROOT.resolve("var");
    private volatile NxFile nxFile;
    @State
    private volatile DeviceState state;
    @Status
    private volatile String status;
    @Pipe
    private volatile PipeValue pipe;
    @Pipe(name = "status")
    private volatile PipeValue statusPipe;
    @DeviceManagement
    private volatile DeviceManager deviceManager;
    @DeviceProperty(defaultValue = "false")
    private boolean stateCheckAttrAlarm = false;
    @Attribute
    private volatile boolean append;
    private ThreadLocal<String> clientId = new ThreadLocal<>();

    {
        logger.debug("XENV_ROOT=" + XENV_ROOT);
    }

    {
        try {
            Files.createDirectories(cwd);
        } catch (IOException e) {
            logger.error("Can not create cwd: " + cwd.toAbsolutePath().toString(), e);
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        ServerManager.getInstance().start(args, DataFormatServer.class);
    }

    static void log(Logger logger, boolean append, String nxPath, String value) {
        logger.debug("{} data to nexus file: {}={}", append ? "Appending" : "Writing", nxPath, value);
    }

    public void setStateCheckAttrAlarm(boolean v) {
        stateCheckAttrAlarm = v;
    }

    public DeviceState getState() {
        return state;
    }

    public void setState(DeviceState newState) {
        state = newState;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public PipeValue getPipe() {
        return pipe;
    }

    public void setPipe(final PipeValue v) throws Exception {
        pipe = v;

        Preconditions.checkState(nxFile != null, "nxFile is null! Is it open?");
        Runnable runnable = null;
        switch (v.getValue().getName()) {
            case "status_server":
                runnable = new WriteTask(new StatusServerBlob(v.getValue()));
                break;
            case "camera":
                runnable = new WriteTask(new CameraBlob(v.getValue()));
                break;
            case "any":
                runnable = new WriteTask(new GenericBlob(v.getValue()));
                break;
            default:
                throw new IllegalArgumentException("Unknown blob type: " + v.getValue().getName());
        }

        exec.execute(runnable);
    }

    public PipeValue getStatusPipe(){
        return statusPipe;
    }

    public void setDeviceManager(DeviceManager manager) {
        deviceManager = manager;
    }

    @Attribute
    public String getNxPath() throws Exception {
        return clientNxPath.get(getClientId());
    }

    @Attribute
    public void setNxPath(String nxPath) throws Exception {
        String clientId = getClientId();

        clientNxPath.put(clientId, nxPath);
    }

    public boolean getAppend() throws Exception {
        return append;
    }

    public void setAppend(boolean v) throws Exception {
        append = v;
    }

    @AroundInvoke
    public void aroundInvoke(InvocationContext ctx) {
        String clientId = ClientIDUtil.toString(ctx.getClientID());
        logger.debug(ctx.toString());
        this.clientId.set(clientId);
    }

    @Attribute
    public String getClientId() throws Exception {
        return clientId.get();
    }

    @Attribute(isMemorized = true)
    public String getCwd() {
        return cwd.toAbsolutePath().toString();
    }

    @Attribute(isMemorized = true)
    public void setCwd(String cwd) {
        Path tmp = XENV_ROOT.resolve(cwd);
        if (!Files.isDirectory(tmp)) throw new IllegalArgumentException("Directory is expected here: " + tmp.toString());
        this.cwd = tmp;

        pushEvent("cwd", tmp.toAbsolutePath().toString());
    }


    @Attribute
    @StateMachine(deniedStates = DeviceState.ON)
    public String getNxFile() {
        return nxFile.getFileName();
    }

    @Attribute(isMemorized = true)
    public String getNxTemplate() {
        return nxTemplate.toAbsolutePath().toString();
    }

    @Attribute(isMemorized = true)
    public void setNxTemplate(String nxTemplateName) {
        Path tmp = XENV_ROOT.resolve(nxTemplateName);
        if (!Files.exists(tmp)) throw new IllegalArgumentException(nxTemplateName + " does not exist.");
        nxTemplate = tmp;

        pushEvent("nxTemplate", nxTemplate.toAbsolutePath().toString());
    }

    @Command
    @StateMachine(endState = DeviceState.STANDBY)
    public void createFile(String fileName) throws Exception {
        String name = cwd.resolve(fileName).toAbsolutePath().toString();
        String nxTemplate = this.nxTemplate.toAbsolutePath().toString();
        nxFile = NxFile.create(name, nxTemplate);
        logger.info("Created file {} using template {}", name, nxTemplate);
    }

    @Command
    @StateMachine(endState = DeviceState.STANDBY)
    public void openFile(String fileName) throws Exception {
        if (nxFile != null) nxFile.close();

        String name = cwd.resolve(fileName).toAbsolutePath().toString();
        nxFile = NxFile.open(name);
        logger.info("Opened file {}", name);
    }

    @Command
    @StateMachine(endState = DeviceState.ON)
    public void closeFile() throws Exception {
        if (nxFile == null) return;
        nxFile.close();
        logger.info("Closed file {}", nxFile.getFileName());
        nxFile = null;
    }

    @Command
    public void writeInteger(final int v) throws Exception {
        final String nxPath = clientNxPath.get(getClientId());

        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        Runnable runnable = new Runnable() {
            public void run() {
                DataFormatServer.this.setState(DeviceState.RUNNING);
                try {
                    log(logger, append, nxPath, String.valueOf(v));
                    nxFile.write(nxPath, v, append);
                    DataFormatServer.this.setState(DeviceState.STANDBY);
                } catch (LibpniioException e) {
                    logger.error(e.getMessage(), e);
                    DataFormatServer.this.setState(DeviceState.FAULT);
                }
            }
        };

        exec.submit(runnable);
    }

    @Command
    public void writeLong(final long v) throws Exception {
        final String nxPath = clientNxPath.get(getClientId());

        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        Runnable runnable = new Runnable() {
            public void run() {
                DataFormatServer.this.setState(DeviceState.RUNNING);
                try {
                    log(logger, append, nxPath, String.valueOf(v));
                    nxFile.write(nxPath, v, append);
                    DataFormatServer.this.setState(DeviceState.STANDBY);
                } catch (LibpniioException e) {
                    logger.error(e.getMessage(), e);
                    DataFormatServer.this.setState(DeviceState.FAULT);
                }
            }

        };

        exec.submit(runnable);
    }

    @Command
    public void writeFloat(final float v) throws Exception {
        final String nxPath = clientNxPath.get(getClientId());

        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        Runnable runnable = new Runnable() {
            public void run() {
                DataFormatServer.this.setState(DeviceState.RUNNING);
                try {
                    log(logger, append, nxPath, String.valueOf(v));
                    nxFile.write(nxPath, v, append);
                    DataFormatServer.this.setState(DeviceState.STANDBY);
                } catch (LibpniioException e) {
                    logger.error(e.getMessage(), e);
                    DataFormatServer.this.setState(DeviceState.FAULT);
                }
            }
        };

        exec.submit(runnable);
    }

    @Command
    public void writeDouble(final double v) throws Exception {
        final String nxPath = clientNxPath.get(getClientId());

        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        Runnable runnable = new Runnable() {
            public void run() {
                DataFormatServer.this.setState(DeviceState.RUNNING);
                try {
                    log(logger, append, nxPath, String.valueOf(v));
                    nxFile.write(nxPath, v, append);
                    DataFormatServer.this.setState(DeviceState.STANDBY);
                } catch (LibpniioException e) {
                    logger.error(e.getMessage(), e);
                    DataFormatServer.this.setState(DeviceState.FAULT);
                }
            }
        };

        exec.submit(runnable);
    }

    @Command
    public void writeString(final String v) throws Exception {
        final String nxPath = clientNxPath.get(getClientId());

        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        Runnable runnable = new Runnable() {
            public void run() {
                DataFormatServer.this.setState(DeviceState.RUNNING);
                try {
                    log(logger, append, nxPath, String.valueOf(v));
                    nxFile.write(nxPath, v, append);
                    DataFormatServer.this.setState(DeviceState.STANDBY);
                } catch (LibpniioException e) {
                    logger.error(e.getMessage(), e);
                    DataFormatServer.this.setState(DeviceState.FAULT);
                }
            }
        };

        exec.submit(runnable);
    }

    @Command
    public void write16bitImage(final short[] data) throws Exception {
        final String nxPath = clientNxPath.get(getClientId());

        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        Runnable runnable = new Runnable() {
            public void run() {
                DataFormatServer.this.setState(DeviceState.RUNNING);
                try {
                    log(logger, append, nxPath, "16 bit image");
                    nxFile.write(nxPath, data, append);
                    DataFormatServer.this.setState(DeviceState.STANDBY);
                } catch (LibpniioException e) {
                    logger.error(e.getMessage(), e);
                    DataFormatServer.this.setState(DeviceState.FAULT);
                }
            }
        };

        exec.submit(runnable);
    }

    @Command
    public void writeARGBImage(final int[] data) throws Exception {
        final String nxPath = clientNxPath.get(getClientId());

        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        Runnable runnable = new Runnable() {
            public void run() {
                DataFormatServer.this.setState(DeviceState.RUNNING);
                try {
                    log(logger, append, nxPath, "ARGB image");
                    nxFile.write(nxPath, data, append);
                    DataFormatServer.this.setState(DeviceState.STANDBY);
                } catch (LibpniioException e) {
                    logger.error(e.getMessage(), e);
                    DataFormatServer.this.setState(DeviceState.FAULT);
                }
            }
        };

        exec.submit(runnable);
    }

    @Command
    public void writeTIFFImage(final float[] data) throws Exception {
        final String nxPath = clientNxPath.get(getClientId());

        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        Runnable runnable = new Runnable() {
            public void run() {
                DataFormatServer.this.setState(DeviceState.RUNNING);
                try {
                    log(logger, append, nxPath, "TIFF image");
                    nxFile.write(nxPath, data, append);
                    DataFormatServer.this.setState(DeviceState.STANDBY);
                } catch (LibpniioException e) {
                    logger.error(e.getMessage(), e);
                    DataFormatServer.this.setState(DeviceState.FAULT);
                }
            }
        };

        exec.submit(runnable);
    }

    @Init
    @StateMachine(endState = DeviceState.ON)
    public void init() throws IOException {

    }

    @Delete
    @StateMachine(endState = DeviceState.OFF)
    public void delete() throws Exception {
        try {
            exec.shutdown();
            exec.awaitTermination(3L, TimeUnit.SECONDS);
            closeFile();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    private void pushEvent(String name, String value) {
        logger.debug("Sending event: {}={}", name, value);

        PipeBlobBuilder pbb = new PipeBlobBuilder(name);
        pbb.add(name, value);

        try {
            deviceManager.pushPipeEvent("status", new PipeValue(pbb.build()));
        } catch (DevFailed devFailed) {
            DevFailedUtils.logDevFailed(devFailed, logger);
        }
    }

    public class WriteTask implements Runnable {
        final NexusWriter writer;

        public WriteTask(NexusWriter writer) {
            this.writer = writer;
        }

        @Override
        public void run() {
            DataFormatServer.this.setState(DeviceState.RUNNING);
            try {
                writer.write(nxFile);
                DataFormatServer.this.setState(DeviceState.STANDBY);
            } catch (IOException e) {
                DataFormatServer.this.logger.error(e.getMessage(), e);
                DataFormatServer.this.setState(DeviceState.FAULT);
            }
        }
    }

    public class StatusPipe {
        public void push() {
            update();
            try {
                deviceManager.pushPipeEvent("status",toPipeValue());
            } catch (DevFailed devFailed) {
                if(getState() == DeviceState.FAULT){
                    DevFailedUtils.logDevFailed(devFailed, logger);
                    return; //give up
                }

                DevFailedUtils.logDevFailed(devFailed, logger);
                setState(DeviceState.FAULT);
                setStatus(DevFailedUtils.toString(devFailed));


                push();
            }
        }

        public void update(){

        }


        public PipeValue toPipeValue(){
            return null;
        }
    }
}