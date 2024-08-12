import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;

@SpringBootTest
public class HandlerTest {

    @InjectMocks
    private Handler handler;

    @Mock
    private SnapshotService snapshotService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testApply() {
        // Arrange
        Map<String, String> event = new HashMap<>();
        event.put("sourceBucketName", "my-source-bucket");
        event.put("sourceFileKey", "gbi/party.csv"); // Updated to include folder and file
        event.put("destinationBucketName", "my-destination-bucket");
        event.put("destinationFileKey", "gbi-report/"); // Updated to only include folder
        event.put("fileTobeProcessed", "someFileType");

        // Mock behavior for SnapshotService methods
        doNothing().when(snapshotService).convertCsvToParquetAndUpload(
                anyString(), anyString(), anyString(), anyString(), anyString());

        // Act
        Map<String, Object> response = handler.apply(event);

        // Assert
        verify(snapshotService).convertCsvToParquetAndUpload(
                "my-source-bucket", "gbi/party.csv", "someFileType", "my-destination-bucket", "gbi-report/");
        assertNotNull(response);
    }
}
