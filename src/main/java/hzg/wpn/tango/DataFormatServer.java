package hzg.wpn.tango;

import com.google.common.base.Preconditions;
import fr.esrf.Tango.DevFailed;
import fr.esrf.TangoApi.PipeBlobBuilder;
import hzg.wpn.nexus.libpniio.jni.LibpniioException;
import hzg.wpn.nexus.libpniio.jni.NxFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.tango.DeviceState;
import org.tango.server.InvocationContext;
import org.tango.server.ServerManager;
import org.tango.server.ServerManagerUtils;
import org.tango.server.annotation.*;
import org.tango.server.attribute.AttributeValue;
import org.tango.server.device.DeviceManager;
import org.tango.server.events.EventType;
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

    public static void main(String[] args) throws IOException {
        ServerManager.getInstance().start(args, DataFormatServer.class);
        ServerManagerUtils.writePidFile(null);
    }

    static void log(Logger logger, boolean append, String nxPath, String value) {
        logger.debug("{} data to nexus file: {}={}", append ? "Appending" : "Writing", nxPath, value);
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
    public void createFile(String fileName) {
        final String name = cwd.resolve(fileName).toAbsolutePath().toString();
        final String nxTemplate = this.nxTemplate.toAbsolutePath().toString();
        exec.submit(() -> {
            MDC.setContextMap(deviceManager.getDevice().getMdcContextMap());
            try {
                nxFile = NxFile.create(name, nxTemplate);
                logger.info("Created file {} using template {}", name, nxTemplate);
                setState(DeviceState.ON);
                setStatus("NxFile=" + name);
            } catch (LibpniioException e) {
                logger.error(String.format("Failed to created file %s using template %s", name, nxTemplate), e);
                setState(DeviceState.FAULT);
                setStatus(String.format("Failed to created file %s using template %s due to %s, %s", name, nxTemplate, e.getClass().getSimpleName(), e.getMessage()));
            }
        });
    }

    @Command
    public void openFile(String fileName) {
        String name = cwd.resolve(fileName).toAbsolutePath().toString();

        exec.submit(() -> {
            MDC.setContextMap(deviceManager.getDevice().getMdcContextMap());
            try {
                if (nxFile != null) nxFile.close();
                nxFile = NxFile.open(name);
                logger.info("Opened file {}", name);
                setState(DeviceState.ON);
                setStatus("NxFile=" + name);
            } catch (LibpniioException| IOException e) {
                logger.error(String.format("Failed to open file %s", name), e);
                setState(DeviceState.FAULT);
                setStatus(String.format("Failed to open file %s due to %s, %s", name, e.getClass().getSimpleName(), e.getMessage()));
            }
        });
    }

    @Command
    public void closeFile() {
        exec.submit(() -> {
            MDC.setContextMap(deviceManager.getDevice().getMdcContextMap());
            try {
                if (nxFile == null) return;
                nxFile.close();
                logger.info("Closed file {}", nxFile.getFileName());
                nxFile = null;
                setState(DeviceState.STANDBY);
                setStatus("Please open or create an NxFile!");
            } catch (IOException e) {
                logger.error(String.format("Failed to closed file %s", nxFile.getFileName()), e);
                setState(DeviceState.FAULT);
                setStatus(String.format("Failed to closed file %s due to %s", nxFile.getFileName(), e.getMessage()));
            }
        });
    }

    @Command
    @StateMachine(deniedStates = {DeviceState.STANDBY, DeviceState.FAULT})
    public void writeInteger(final int v) throws Exception {
        final String nxPath = clientNxPath.get(getClientId());

        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        exec.submit(new WriteTask(new IntegerWriter(v, nxPath)));
    }

    @Command
    @StateMachine(deniedStates = {DeviceState.STANDBY, DeviceState.FAULT})
    public void writeLong(final long v) throws Exception {
        final String nxPath = clientNxPath.get(getClientId());

        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        exec.submit(new WriteTask(new LongWriter(v, nxPath)));
    }


    @Command
    @StateMachine(deniedStates = {DeviceState.STANDBY, DeviceState.FAULT})
    public void writeFloat(final float v) throws Exception {
        final String nxPath = clientNxPath.get(getClientId());

        Preconditions.checkState(nxPath == null || nxPath.isEmpty(), "nxPath must be set before calling this command!");

        exec.submit(new WriteTask(new FloatWriter(v, nxPath)));
    }

    @Command
    @StateMachine(deniedStates = {DeviceState.STANDBY, DeviceState.FAULT})
    public void writeDouble(final double v) throws Exception {
        final String nxPath = clientNxPath.get(getClientId());

        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        exec.submit(new WriteTask(new DoubleWriter(v, nxPath)));
    }

    @Command
    @StateMachine(deniedStates = {DeviceState.STANDBY, DeviceState.FAULT})
    public void writeString(final String v) throws Exception {
        final String nxPath = clientNxPath.get(getClientId());

        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        exec.submit(new WriteTask(new StringWriter(v, nxPath)));
    }

    @Init
    @StateMachine(endState = DeviceState.STANDBY)
    public void init() {

    }

    @Delete
    @StateMachine(endState = DeviceState.OFF)
    public void delete() throws Exception {
        try {
            closeFile();
            exec.shutdown();
            exec.awaitTermination(3L, TimeUnit.SECONDS);
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

    private void setAndPushStatus(String status) throws DevFailed {
        setStatus(status);
        deviceManager.pushEvent("Status", new AttributeValue(status), EventType.CHANGE_EVENT);
    }

    private class DoubleWriter implements NexusWriter {
        private double v;
        private String nxPath;

        private DoubleWriter(double v, String nxPath) {
            this.v = v;
            this.nxPath = nxPath;
        }

        @Override
        public void write(NxFile file) throws IOException {
            try {
                nxFile.write(nxPath, v, append);
            } catch (LibpniioException e) {
                throw new IOException(e);
            }
        }
    }

    private class FloatWriter implements NexusWriter {
        private float v;
        private String nxPath;

        private FloatWriter(float v, String nxPath) {
            this.v = v;
            this.nxPath = nxPath;
        }

        @Override
        public void write(NxFile file) throws IOException {
            try {
                nxFile.write(nxPath, v, append);
            } catch (LibpniioException e) {
                throw new IOException(e);
            }
        }
    }

    private class IntegerWriter implements NexusWriter {
        private int v;
        private String nxPath;

        private IntegerWriter(int v, String nxPath) {
            this.v = v;
            this.nxPath = nxPath;
        }

        @Override
        public void write(NxFile file) throws IOException {
            try {
                nxFile.write(nxPath, v, append);
            } catch (LibpniioException e) {
                throw new IOException(e);
            }
        }
    }

    private class LongWriter implements NexusWriter {
        private long v;
        private String nxPath;


        private LongWriter(long v, String nxPath) {
            this.v = v;
            this.nxPath = nxPath;
        }

        @Override
        public void write(NxFile file) throws IOException {
            try {
                nxFile.write(nxPath, v, append);
            } catch (LibpniioException e) {
                throw new IOException(e);
            }
        }
    }

    private class StringWriter implements NexusWriter {
        private String v;
        private String nxPath;

        private StringWriter(String v, String nxPath) {
            this.v = v;
            this.nxPath = nxPath;
        }

        @Override
        public void write(NxFile file) throws IOException {
            try {
                file.write(nxPath, v, append);
            } catch (LibpniioException e) {
                throw new IOException(e);
            }
        }
    }

    public class WriteTask implements Runnable {
        final NexusWriter writer;

        public WriteTask(NexusWriter writer) {
            this.writer = new PushStatusWriter(writer);
        }

        @Override
        public void run() {
            MDC.setContextMap(deviceManager.getDevice().getMdcContextMap());
            DataFormatServer.this.setState(DeviceState.RUNNING);
            try {
                writer.write(nxFile);
                DataFormatServer.this.setState(DeviceState.ON);
            } catch (IOException e) {
                DataFormatServer.this.logger.error(e.getMessage(), e);
                DataFormatServer.this.setState(DeviceState.ALARM);
                DataFormatServer.this.setStatus(String.format("Failed to write into %s due to %s", nxFile.getFileName(), e.getMessage()));
            }
        }
    }

    private class PushStatusWriter implements NexusWriter {
        private NexusWriter nested;

        private PushStatusWriter(NexusWriter nested) {
            this.nested = nested;
        }

        @Override
        public void write(NxFile file) throws IOException {
            try {
                DataFormatServer.this.setAndPushStatus(String.format("Performing %s.write into %s", nested.getClass().getSimpleName(), file.getFileName()));
            } catch (DevFailed e) {
                DevFailedUtils.logDevFailed(e, logger);
            }
            nested.write(file);
            try {
                DataFormatServer.this.setAndPushStatus(String.format("Done %s.write into %s", nested.getClass().getSimpleName(), file.getFileName()));
            } catch (DevFailed e) {
                DevFailedUtils.logDevFailed(e, logger);
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