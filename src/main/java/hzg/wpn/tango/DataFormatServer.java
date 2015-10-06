package hzg.wpn.tango;

import com.google.common.base.Preconditions;
import fr.esrf.Tango.DevFailed;
import fr.esrf.TangoApi.PipeBlobBuilder;
import hzg.wpn.nexus.libpniio.jni.NxFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tango.DeviceState;
import org.tango.server.InvocationContext;
import org.tango.server.ServerManager;
import org.tango.server.annotation.*;
import org.tango.server.attribute.AttributeValue;
import org.tango.server.device.DeviceManager;
import org.tango.server.events.EventType;
import org.tango.server.pipe.PipeValue;
import org.tango.utils.ClientIDUtil;
import org.tango.utils.DevFailedUtils;

import javax.annotation.concurrent.Immutable;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author khokhria
 * @since 6/16/15.
 */
@Device(transactionType = TransactionType.NONE)
public class DataFormatServer {
    private static final Logger logger = LoggerFactory.getLogger(DataFormatServer.class);
    private static final ExecutorService exec = Executors.newSingleThreadExecutor();

    private static final Path XENV_ROOT;

    static {
        String xenv_root;

        XENV_ROOT = Paths.get(
                (xenv_root = System.getProperty("XENV_ROOT", System.getenv("XENV_ROOT"))) == null ? "" : xenv_root);

        logger.debug("XENV_ROOT=" + XENV_ROOT);
    }

    private final ConcurrentMap<String, WriteContext> clientWriteContexts = new ConcurrentHashMap<>();
    private volatile Path nxTemplate = XENV_ROOT.resolve("etc/default.nxdl.xml");
    private volatile Path cwd = XENV_ROOT.resolve("var");
    private volatile NxFile nxFile;
    @State
    private volatile DeviceState state;
    @Pipe
    private volatile PipeValue pipe;
    @DeviceManagement
    private volatile DeviceManager deviceManager;
    @Attribute(isMemorized = true)
    private volatile boolean append;
    {
        try {
            Files.createDirectories(cwd);
        } catch (IOException e) {
            logger.error("Can not create cwd: " + cwd.toAbsolutePath().toString(), e);
            throw new RuntimeException(e);
        }
    }

    private ThreadLocal<String> clientId = new ThreadLocal<>();

    public static void main(String[] args) {
        ServerManager.getInstance().start(args, DataFormatServer.class);
    }

    public DeviceState getState() {
        return state;
    }

    public void setState(DeviceState newState) {
        state = newState;
        try {
            deviceManager.pushEvent("State", new AttributeValue(newState.getDevState()), EventType.CHANGE_EVENT);
        } catch (DevFailed devFailed) {
            DevFailedUtils.logDevFailed(devFailed, logger);
            state = DeviceState.FAULT;
        }
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
                runnable = new WriteTask(new CameraBlob(v.getValue(), append));
                break;
            case "any":
                runnable = new WriteTask(new GenericBlob(v.getValue(), append));
                break;
            default:
                throw new IllegalArgumentException("Unknown blob type: " + v.getValue().getName());
        }

        exec.submit(runnable);
    }

    public void setDeviceManager(DeviceManager manager) {
        deviceManager = manager;
    }

    @Attribute
    public String getNxPath() throws Exception {
        return clientWriteContexts.get(getClientId()).nxPath;
    }

    @Attribute
    public void setNxPath(String nxPath) throws Exception {
        String clientId = getClientId();
        final WriteContext ctx = getClientContext(clientId);

        if (!nxPath.equals(ctx.nxPath)) {
            logger.debug("submit new write task");
            exec.submit(new WriteTask(ctx.toGenericBlob()));

            WriteContext newCtx = new WriteContext(nxPath);

            logger.debug("Setting nx_path=" + nxPath);
            clientWriteContexts.replace(clientId, newCtx);
        }

    }

    public boolean getAppend() throws Exception {
        return append;
    }

    public void setAppend(boolean v) throws Exception {
        append = v;
    }

    private WriteContext getClientContext(String clientId) {
        WriteContext ctx = clientWriteContexts.get(clientId);

        if (ctx == null)
            clientWriteContexts.put(clientId, new WriteContext(null));

        return ctx;
    }

    @AroundInvoke
    public void aroundInvoke(InvocationContext ctx) {
        String clientId = ClientIDUtil.toString(ctx.getClientID());
        logger.debug("clientId={}", clientId);
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

    @Command
    @StateMachine(endState = DeviceState.STANDBY)
    public void createFile(String fileName) throws Exception {
        nxFile = NxFile.create(cwd.resolve(fileName).toAbsolutePath().toString(), nxTemplate.toAbsolutePath().toString());
    }

    @Command
    @StateMachine(endState = DeviceState.STANDBY)
    public void openFile(String fileName) throws Exception {
        if (nxFile != null) nxFile.close();

        nxFile = NxFile.open(cwd.resolve(fileName).toAbsolutePath().toString());
    }

    @Command
    @StateMachine(endState = DeviceState.ON)
    public void closeFile() throws Exception {
        if (nxFile == null) return;
        nxFile.close();
        nxFile = null;
    }

    @Command
    @StateMachine(endState = DeviceState.STANDBY)
    public void writeInteger(int v) throws Exception {
        WriteContext writeContext = getClientContext(getClientId());
        String nxPath = writeContext.nxPath;

        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        writeContext.addValue(v);
    }

    @Command
    @StateMachine(endState = DeviceState.STANDBY)
    public void writeLong(long v) throws Exception {
        WriteContext writeContext = getClientContext(getClientId());
        String nxPath = writeContext.nxPath;

        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        writeContext.addValue(v);
    }

    @Command
    @StateMachine(endState = DeviceState.STANDBY)
    public void writeFloat(float v) throws Exception {
        WriteContext writeContext = getClientContext(getClientId());
        String nxPath = writeContext.nxPath;

        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        writeContext.addValue(v);
    }

    @Command
    @StateMachine(endState = DeviceState.STANDBY)
    public void writeDouble(double v) throws Exception {
        WriteContext writeContext = getClientContext(getClientId());
        String nxPath = writeContext.nxPath;

        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        writeContext.addValue(v);
    }

    @Command
    @StateMachine(endState = DeviceState.STANDBY)
    public void writeString(String v) throws Exception {
        WriteContext writeContext = getClientContext(getClientId());
        String nxPath = writeContext.nxPath;

        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        writeContext.addValue(v);
    }

    @Command
    @StateMachine(endState = DeviceState.STANDBY)
    public void write16bitImage(short[] data) throws Exception {
        WriteContext writeContext = getClientContext(getClientId());
        String nxPath = writeContext.nxPath;

        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        logger.debug("Writing 16 bit image to " + nxPath);
        setState(DeviceState.RUNNING);
        nxFile.write(nxPath, data, append);
    }

    @Command
    @StateMachine(endState = DeviceState.STANDBY)
    public void writeARGBImage(int[] data) throws Exception {
        WriteContext writeContext = getClientContext(getClientId());
        String nxPath = writeContext.nxPath;

        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        logger.debug("Writing ARGB image to " + nxPath);
        setState(DeviceState.RUNNING);
        nxFile.write(nxPath, data, append);
    }

    @Command
    @StateMachine(endState = DeviceState.STANDBY)
    public void writeTIFFImage(float[] data) throws Exception {
        WriteContext writeContext = getClientContext(getClientId());
        String nxPath = writeContext.nxPath;

        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        logger.debug("Writing float image to " + nxPath);
        setState(DeviceState.RUNNING);
        nxFile.write(nxPath, data, append);
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

    @Immutable
    public class WriteContext {
        final String nxPath;
        final AtomicReference<Object> valuesRef = new AtomicReference<>();

        private WriteContext(String nxPath) {
            this.nxPath = nxPath;
        }

        private int ensureCapacity(Class<?> type) {
            Object values = valuesRef.get();
            if (values == null) {
                valuesRef.set(Array.newInstance(type, 1));
                return 0;
            } else {
                int length = Array.getLength(values);
                valuesRef.set(Array.newInstance(type, length + 1));
                return length;
            }
        }

        void addValue(int v) {
            int ndx = ensureCapacity(int.class);
            Array.setInt(valuesRef.get(), ndx, v);
        }

        void addValue(long v) {
            int ndx = ensureCapacity(long.class);
            Array.setLong(valuesRef.get(), ndx, v);
        }

        void addValue(float v) {
            int ndx = ensureCapacity(float.class);
            Array.setFloat(valuesRef.get(), ndx, v);
        }

        void addValue(double v) {
            int ndx = ensureCapacity(float.class);
            Array.setDouble(valuesRef.get(), ndx, v);
        }

        void addValue(String v) {
            int ndx = ensureCapacity(float.class);
            Array.set(valuesRef.get(), ndx, v);
        }


        GenericBlob toGenericBlob() throws Exception {
            PipeBlobBuilder pipeBlobBuilder = new PipeBlobBuilder(getClientId());

            PipeBlobBuilder inner = new PipeBlobBuilder(nxPath);

            inner.add(nxPath, valuesRef.get());

            pipeBlobBuilder.add(nxPath, inner.build());

            return new GenericBlob(pipeBlobBuilder.build(), DataFormatServer.this.append);
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
            } catch (IOException e) {
                DataFormatServer.logger.error(e.getMessage(), e);
                DataFormatServer.this.setState(DeviceState.FAULT);
            }
        }
    }
}