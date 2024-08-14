import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class SnapshotServiceImplTest {

    @Mock
    private S3Client s3Client;

    @InjectMocks
    private SnapshotServiceImpl snapshotService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testConvertCsvToParquetAndUpload() throws IOException {
        // Setup
        List<String[]> csvData = Arrays.asList(
            new String[]{"id", "name"},
            new String[]{"1", "Alice"},
            new String[]{"2", "Bob"}
        );

        // Mock the method
        when(snapshotService.readCsvFromS3(anyString(), anyString())).thenReturn(csvData);

        // Mock the S3 putObject method
        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenReturn(null);

        // Execute the method under test
        snapshotService.convertCsvToParquetAndUpload(
            "source-bucket", "gbi/party.csv", "someFileType", "destination-bucket", "gbi-report/"
        );

   
