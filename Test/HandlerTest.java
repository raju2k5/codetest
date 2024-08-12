import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import java.util.HashMap;
import java.util.Map;

public class HandlerTest {

    @Mock
    private SnapshotService snapshotService;

    @InjectMocks
    private Handler handler;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testApply_success() {
        // Arrange
        Map<String, String> event = new HashMap<>();
        event.put("sourceBucketName", "my-source-bucket");
        event.put("sourceFileKey", "gbi/party.csv");
        event.put("destinationBucketName", "my-destination-bucket");
        event.put("destinationFileKey", "gbi-report/");
        event.put("fileTobeProcessed", "someFileType");

        // Act
        handler.apply(event);

        // Assert
        verify(snapshotService).convertCsvToParquetAndUpload(
                "my-source-bucket", "gbi/party.csv", "someFileType", "my-destination-bucket", "gbi-report/");
    }

    @Test
    public void testApply_exception() {
        // Arrange
        Map<String, String> event = new HashMap<>();
        event.put("sourceBucketName", "my-source-bucket");
        event.put("sourceFileKey", "gbi/party.csv");
        event.put("destinationBucketName", "my-destination-bucket");
        event.put("destinationFileKey", "gbi-report/");
        event.put("fileTobeProcessed", "someFileType");

        doThrow(new RuntimeException("Exception during file conversion"))
                .when(snapshotService)
                .convertCsvToParquetAndUpload(anyString(), anyString(), anyString(), anyString(), anyString());

        // Act & Assert
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> {
            handler.apply(event);
        });

        assertEquals("Exception occured during file convertion..", thrown.getMessage());
    }
}
