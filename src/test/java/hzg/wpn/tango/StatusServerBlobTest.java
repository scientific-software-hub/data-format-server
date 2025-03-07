package hzg.wpn.tango;

import fr.esrf.TangoApi.PipeBlob;
import fr.esrf.TangoApi.PipeBlobBuilder;
import org.waltz.nexus.NxFile;
import org.junit.Test;

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

        verify(mockFile, atLeastOnce()).write("/entry/int/value", 1, true);
        verify(mockFile, atLeastOnce()).write("/entry/int/value", 2, true);
        verify(mockFile, atLeastOnce()).write("/entry/int/value", 3, true);
        verify(mockFile, atLeast(3)).write(eq("/entry/int/time"), anyLong(), eq(true));

        verify(mockFile, atLeastOnce()).write("/entry/long/value", 1001L, true);
        verify(mockFile, atLeastOnce()).write("/entry/long/value", 1002L, true);
        verify(mockFile, atLeastOnce()).write("/entry/long/value", 1003L, true);
        verify(mockFile, atLeast(3)).write(eq("/entry/long/time"), anyLong(), eq(true));

        verify(mockFile, atLeastOnce()).write("/entry/float/value", 3.14F, true);
        verify(mockFile, atLeastOnce()).write("/entry/float/value", 2.78F, true);
        verify(mockFile, atLeast(2)).write(eq("/entry/float/time"), anyLong(), eq(true));
    }
}