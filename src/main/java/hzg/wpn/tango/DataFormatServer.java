package hzg.wpn.tango;

import com.google.common.base.Preconditions;
import fr.esrf.TangoApi.PipeBlob;
import fr.esrf.TangoApi.PipeBlobBuilder;
import hzg.wpn.nexus.libpniio.jni.LibpniioException;
import hzg.wpn.nexus.libpniio.jni.NxFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.tango.DeviceState;
import org.tango.server.InvocationContext;
import org.tango.server.ServerManager;
import org.tango.server.annotation.*;
import org.tango.server.device.DeviceManager;
import org.tango.server.pipe.PipeValue;
import org.tango.utils.ClientIDUtil;

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
                (xenv_root = System.getProperty("XENV_ROOT", System.getenv("XENV_ROOT"))) == null ? System.getProperty("user.dir") : xenv_root);


    }

    private final Logger logger = LoggerFactory.getLogger(DataFormatServer.class);
    private volatile ExecutorService exec;;
    //clientId -> nxPath
    private final ConcurrentMap<String, String> clientNxPath = new ConcurrentHashMap<>();
    private volatile Path nxTemplate = XENV_ROOT.resolve("etc/default.nxdl.xml");
    private volatile NxFile nxFile;
    @State
    private volatile DeviceState state;
    @Status
    private volatile String status;
    @Pipe
    private volatile PipeValue pipe;
    @DeviceManagement
    private volatile DeviceManager deviceManager;

    private ThreadLocal<String> clientId = new ThreadLocal<>();

    {
        logger.debug("XENV_ROOT=" + XENV_ROOT);
    }

    public static void main(String[] args) {
        ServerManager.getInstance().start(args, DataFormatServer.class);
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
            case "any":
                runnable = new WriteTask(new GenericBlob(v.getValue()));
                break;
            default:
                throw new IllegalArgumentException("Unknown blob type: " + v.getValue().getName());
        }

        exec.execute(runnable);
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
        //TODO aroundInvoke?
        if(NexusWriterHelper.hasMapping("external:" + nxPath)) {
            nxPath = NexusWriterHelper.toNxPath("external:" + nxPath);
        }
        clientNxPath.put(clientId, nxPath);
    }

    @AroundInvoke
    public void aroundInvoke(InvocationContext ctx) {
        String clientId = ClientIDUtil.toString(ctx.getClientID());
        logger.debug(ctx.toString());
        this.clientId.set(clientId);
    }

    @Attribute
    public String getClientId() {
        return clientId.get();
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
        if (!Files.exists(tmp)) throw new IllegalArgumentException(String.format("NeXus template %s does not exist.", nxTemplateName));
        nxTemplate = tmp;
    }

    @Command(inTypeDesc = "Absolute or relative output file name")
    public void createFile(String fileName) {
        final String name = Paths.get(fileName).toAbsolutePath().toString();
        final String nxTemplate = this.nxTemplate.toAbsolutePath().toString();
        exec.submit(() -> {
            MDC.setContextMap(deviceManager.getDevice().getMdcContextMap());
            try {
                nxFile = NxFile.create(name, nxTemplate);
                logger.debug("Created file {} using template {}", name, nxTemplate);
                deviceManager.pushStateChangeEvent(DeviceState.ON);
                deviceManager.pushStatusChangeEvent("NxFile=" + name);
            } catch (LibpniioException e) {
                logger.error(String.format("Failed to created file %s using template %s", name, nxTemplate), e);
                deviceManager.pushStateChangeEvent(DeviceState.FAULT);
                deviceManager.pushStatusChangeEvent(String.format("Failed to created file %s using template %s due to %s, %s", name, nxTemplate, e.getClass().getSimpleName(), e.getMessage()));
            }
        });
    }

    @Command(inTypeDesc = "Absolute or relative output file name")
    public void openFile(String fileName) {
        String name = Paths.get(fileName).toAbsolutePath().toString();

        exec.submit(() -> {
            MDC.setContextMap(deviceManager.getDevice().getMdcContextMap());
            try {
                if (nxFile != null) nxFile.close();
                nxFile = NxFile.open(name);
                logger.debug("Opened file {}", name);
                deviceManager.pushStateChangeEvent(DeviceState.ON);
                deviceManager.pushStatusChangeEvent("NxFile=" + name);
            } catch (LibpniioException| IOException e) {
                logger.error(String.format("Failed to open file %s", name), e);
                deviceManager.pushStateChangeEvent(DeviceState.FAULT);
                deviceManager.pushStatusChangeEvent(String.format("Failed to open file %s due to %s, %s", name, e.getClass().getSimpleName(), e.getMessage()));
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
                deviceManager.pushStateChangeEvent(DeviceState.STANDBY);
                deviceManager.pushStatusChangeEvent("Please open or create an NxFile!");
            } catch (IOException e) {
                logger.error(String.format("Failed to closed file %s", nxFile.getFileName()), e);
                deviceManager.pushStateChangeEvent(DeviceState.FAULT);
                deviceManager.pushStatusChangeEvent(String.format("Failed to closed file %s due to %s", nxFile.getFileName(), e.getMessage()));
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
    public void appendInteger(final int v) throws Exception {
        final String nxPath = clientNxPath.get(getClientId());

        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        exec.submit(new WriteTask(new IntegerAppender(v, nxPath)));
    }

    @Command
    @StateMachine(deniedStates = {DeviceState.STANDBY, DeviceState.FAULT})
    public void writeIntegerArray(final int[] v) throws Exception {
        final String nxPath = clientNxPath.get(getClientId());

        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        exec.submit(
                new WriteTask(
                        new GenericBlob(
                                toPipeBlob(nxPath, v)
                        )));
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
    public void appendLong(final long v) throws Exception {
        final String nxPath = clientNxPath.get(getClientId());

        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        exec.submit(new WriteTask(new LongAppender(v, nxPath)));
    }

    @Command
    @StateMachine(deniedStates = {DeviceState.STANDBY, DeviceState.FAULT})
    public void writeLongArray(final long[] v) throws Exception {
        final String nxPath = clientNxPath.get(getClientId());

        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        exec.submit(
                new WriteTask(
                        new GenericBlob(
                                toPipeBlob(nxPath, v)
                        )));
    }

    private PipeBlob toPipeBlob(String nxPath, Object array) {
        return new PipeBlobBuilder("DataBlob")
                .add("append", true)
                .add("data",
                        new PipeBlobBuilder(nxPath)
                                .add(nxPath, wrapArray(nxPath, array))
                                .build())
                .build();
    }

    private PipeBlob wrapArray(String nxPath, Object v) {
        //inner blob
        return new PipeBlobBuilder(nxPath).add("Array", v).build();
    }


    @Command
    @StateMachine(deniedStates = {DeviceState.STANDBY, DeviceState.FAULT})
    public void writeFloat(final float v) throws Exception {
        final String nxPath = clientNxPath.get(getClientId());

        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        exec.submit(new WriteTask(new FloatWriter(v, nxPath)));
    }

    @Command
    @StateMachine(deniedStates = {DeviceState.STANDBY, DeviceState.FAULT})
    public void appendFloat(final float v) throws Exception {
        final String nxPath = clientNxPath.get(getClientId());

        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        exec.submit(new WriteTask(new FloatAppender(v, nxPath)));
    }

    @Command
    @StateMachine(deniedStates = {DeviceState.STANDBY, DeviceState.FAULT})
    public void writeFloatArray(final float[] v) throws Exception {
        final String nxPath = clientNxPath.get(getClientId());

        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        exec.submit(
                new WriteTask(
                        new GenericBlob(
                                toPipeBlob(nxPath, v)
                        )));
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
    public void appendDouble(final double v) throws Exception {
        final String nxPath = clientNxPath.get(getClientId());

        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        exec.submit(new WriteTask(new DoubleAppender(v, nxPath)));
    }

    @Command
    @StateMachine(deniedStates = {DeviceState.STANDBY, DeviceState.FAULT})
    public void writeDoubleArray(final double[] v) throws Exception {
        final String nxPath = clientNxPath.get(getClientId());

        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        exec.submit(
                new WriteTask(
                        new GenericBlob(
                                toPipeBlob(nxPath, v)
                        )));
    }

    @Command
    @StateMachine(deniedStates = {DeviceState.STANDBY, DeviceState.FAULT})
    public void writeString(final String v) throws Exception {
        final String nxPath = clientNxPath.get(getClientId());

        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        exec.submit(new WriteTask(new StringWriter(v, nxPath)));
    }

    @Command
    @StateMachine(deniedStates = {DeviceState.STANDBY, DeviceState.FAULT})
    public void appendString(final String v) throws Exception {
        final String nxPath = clientNxPath.get(getClientId());

        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        exec.submit(new WriteTask(new StringAppender(v, nxPath)));
    }

    @Command
    @StateMachine(deniedStates = {DeviceState.STANDBY, DeviceState.FAULT})
    public void writeStringArray(final String[] v) throws Exception {
        final String nxPath = clientNxPath.get(getClientId());

        if (nxPath == null || nxPath.isEmpty())
            throw new IllegalStateException("nxPath must be set before calling this command!");

        exec.submit(
                new WriteTask(
                        new GenericBlob(
                                toPipeBlob(nxPath, v)
                        )));
    }

    @Init
    @StateMachine(endState = DeviceState.STANDBY)
    public void init() {
        exec = Executors.newSingleThreadExecutor();
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

    private class DoubleWriter extends NexusWriter {
        private double v;

        private DoubleWriter(double v, String nxPath) {
            super(nxPath);
            this.v = v;
        }

        @Override
        public void write(NxFile file) throws IOException {
            try {
                nxFile.write(nxPath, v);
            } catch (LibpniioException e) {
                throw new IOException(e);
            }
        }
    }

    private class DoubleAppender extends NexusWriter {
        private double v;

        private DoubleAppender(double v, String nxPath) {
            super(nxPath);
            this.v = v;
        }

        @Override
        public void write(NxFile file) throws IOException {
            try {
                nxFile.write(nxPath, v, true);
            } catch (LibpniioException e) {
                throw new IOException(e);
            }
        }
    }

    private class FloatWriter extends NexusWriter {
        private float v;

        private FloatWriter(float v, String nxPath) {
            super(nxPath);
            this.v = v;
        }

        @Override
        public void write(NxFile file) throws IOException {
            try {
                nxFile.write(nxPath, v);
            } catch (LibpniioException e) {
                throw new IOException(e);
            }
        }
    }

    private class FloatAppender extends NexusWriter {
        private float v;

        private FloatAppender(float v, String nxPath) {
            super(nxPath);
            this.v = v;
        }

        @Override
        public void write(NxFile file) throws IOException {
            try {
                nxFile.write(nxPath, v, true);
            } catch (LibpniioException e) {
                throw new IOException(e);
            }
        }
    }

    private class IntegerWriter extends NexusWriter {
        private int v;

        private IntegerWriter(int v, String nxPath) {
            super(nxPath);
            this.v = v;
        }

        @Override
        public void write(NxFile file) throws IOException {
            try {
                nxFile.write(nxPath, v);
            } catch (LibpniioException e) {
                throw new IOException(e);
            }
        }
    }

    private class IntegerAppender extends NexusWriter {
        private int v;

        private IntegerAppender(int v, String nxPath) {
            super(nxPath);
            this.v = v;
        }

        @Override
        public void write(NxFile file) throws IOException {
            try {
                nxFile.write(nxPath, v, true);
            } catch (LibpniioException e) {
                throw new IOException(e);
            }
        }
    }

    private class LongWriter extends NexusWriter {
        long v;


        private LongWriter(long v, String nxPath) {
            super(nxPath);
            this.v = v;
        }

        @Override
        public void write(NxFile file) throws IOException {
            try {
                nxFile.write(nxPath, v);
            } catch (LibpniioException e) {
                throw new IOException(e);
            }
        }
    }

    private class LongAppender extends NexusWriter {
        long v;


        private LongAppender(long v, String nxPath) {
            super(nxPath);
            this.v = v;
        }

        @Override
        public void write(NxFile file) throws IOException {
            try {
                nxFile.write(nxPath, v, true);
            } catch (LibpniioException e) {
                throw new IOException(e);
            }
        }
    }

    private class StringWriter extends NexusWriter {
        private String v;

        private StringWriter(String v, String nxPath) {
            super(nxPath);
            this.v = v;
        }

        @Override
        public void write(NxFile file) throws IOException {
            try {
                file.write(nxPath, v);
            } catch (LibpniioException e) {
                throw new IOException(e);
            }
        }
    }

    private class StringAppender extends NexusWriter {
        private String v;

        private StringAppender(String v, String nxPath) {
            super(nxPath);
            this.v = v;
        }

        @Override
        public void write(NxFile file) throws IOException {
            try {
                file.write(nxPath, v, true);
            } catch (LibpniioException e) {
                throw new IOException(e);
            }
        }
    }

    public class WriteTask implements Runnable {
        final NexusWriter writer;

        public WriteTask(NexusWriter writer) {
            this.writer = writer;
        }

        @Override
        public void run() {
            MDC.setContextMap(deviceManager.getDevice().getMdcContextMap());
            logger.debug("Performing {}.write into {}", writer.getClass().getSimpleName(), nxFile.getFileName());
            try {
                writer.write(nxFile);
//                nxFile.flush();
            } catch (IOException /*| LibpniioException */e) {
                DataFormatServer.this.logger.error(e.getMessage(), e);
                deviceManager.pushStateChangeEvent(DeviceState.ALARM);
                deviceManager.pushStatusChangeEvent(String.format("Failed to write a value into %s in %s due to %s", writer.nxPath ,nxFile.getFileName(), e.getMessage()));
            }
        }
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
        this.status = String.format("%d: %s",System.currentTimeMillis(), status);
    }
}
