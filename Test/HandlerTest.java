import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

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
        event.put("sourceFileKey", "gbi/party.csv");
        event.put("destinationBucketName", "my-destination-bucket");
        event.put("destinationFileKey", "gbi-report/");
        event.put("fileTobeProcessed", "someFileType");

        // Mock behavior for SnapshotService methods
        doNothing().when(snapshotService).convertCsvToParquetAndUpload(
                eq("my-source-bucket"), eq("gbi/party.csv"), eq("someFileType"), eq("my-destination-bucket"), eq("gbi-report/"));

        // Act
        Map<String, Object> response = handler.apply(event);

        // Assert
        verify(snapshotService).convertCsvToParquetAndUpload(
                eq("my-source-bucket"), eq("gbi/party.csv"), eq("someFileType"), eq("my-destination-bucket"), eq("gbi-report/"));
        assertNotNull(response);
    }
}
