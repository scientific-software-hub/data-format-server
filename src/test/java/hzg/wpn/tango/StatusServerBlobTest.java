package hzg.wpn.tango;

import fr.esrf.TangoApi.PipeBlob;
import fr.esrf.TangoApi.PipeBlobBuilder;
import org.junit.Test;
import org.waltz.nexus.NxFile;

import static org.mockito.Mockito.*;

/**
 * @author khokhria
 * @since 7/15/15.
 */
public class StatusServerBlobTest {
    private NxFile mockFile = mock(NxFile.class);

    @Test
    public void testWrite() throws Exception {
        PipeBlob testBlob = new PipeBlobBuilder("status_server")
                .add("int-blob", new PipeBlobBuilder("/entry/int").add("value", new int[]{1, 2, 3}, false).add("time", new long[]{100L, 101L, 100L}).build())
                .add("long-blob", new PipeBlobBuilder("/entry/long").add("value", new long[]{1001L, 1002L, 1003L}).add("time", new long[]{100L, 101L, 100L}).build())
                .add("float-blob", new PipeBlobBuilder("/entry/float").add("value", new float[]{3.14F, 2.78F}).add("time", new long[]{100L, 101L}).build())
                .build();


        StatusServerBlob instance = new StatusServerBlob(testBlob);

        instance.write(mockFile);

        verify(mockFile, atLeastOnce()).append("/entry/int/value", new int[]{1, 2, 3});
        verify(mockFile, atLeastOnce()).append(eq("/entry/int/time"), anyObject());

        verify(mockFile, atLeastOnce()).append("/entry/long/value", new long[]{1001L, 1002L, 1003L});
        verify(mockFile, atLeastOnce()).append(eq("/entry/long/time"), anyObject());

        verify(mockFile, atLeastOnce()).append("/entry/float/value", new float[]{3.14F, 2.78F});
        verify(mockFile, atLeastOnce()).append(eq("/entry/float/time"), anyObject());
    }
}