import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.core.exception.SdkClientException;
import com.opencsv.CSVReader;
import java.io.*;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SnapshotServiceImplTest {

    @Mock
    private S3Client s3Client;

    @InjectMocks
    private SnapshotServiceImpl snapshotService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        snapshotService = spy(snapshotService);  // Create a spy for partial mocking
    }

    @Test
    void testConvertCsvToParquetAndUpload_Success() throws IOException {
        String sourceBucketName = "source-bucket";
        String sourceFileKey = "source.csv";
        String fileTobeProcessed = "gbi_party";
        String destinationBucketName = "destination-bucket";
        String destinationFileKey = "destination.parquet";

        // Mock S3 response
        ResponseInputStream<GetObjectResponse> responseInputStream = mock(ResponseInputStream.class);
        when(s3Client.getObject(any(GetObjectRequest.class))).thenReturn(responseInputStream);

        // Mock the CSVReader behavior to simulate streaming CSV rows
        CSVReader csvReader = mock(CSVReader.class);
        when(csvReader.readNext())
                .thenReturn(new String[]{"header1", "header2"})   // First call returns headers
                .thenReturn(new String[]{"value1", "value2"})    // Second call returns first row of data
                .thenReturn(null);  // End of file

        doReturn(csvReader).when(snapshotService).readCsvFromS3(anyString(), anyString());

        // Mock Schema loading
        doReturn("{\"type\": \"record\", \"name\": \"test\", \"fields\": [{\"name\": \"header1\", \"type\": \"string\"}, {\"name\": \"header2\", \"type\": \"string\"}]}").when(snapshotService).loadJsonSchema(anyString());

        // Mock S3 putObject behavior for Parquet file upload
        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenReturn(null);

        // Perform the test and ensure no exceptions are thrown
        assertDoesNotThrow(() -> snapshotService.convertCsvToParquetAndUpload(sourceBucketName, sourceFileKey, fileTobeProcessed, destinationBucketName, destinationFileKey));

        // Verify interactions with S3
        verify(s3Client).getObject(any(GetObjectRequest.class));  // Ensure the CSV file is read from S3
        verify(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));  // Ensure the Parquet file is uploaded to S3
    }

    @Test
    void testConvertCsvToParquetAndUpload_S3ClientException() throws IOException {
        String sourceBucketName = "source-bucket";
        String sourceFileKey = "source.csv";
        String fileTobeProcessed = "gbi_party";
        String destinationBucketName = "destination-bucket";
        String destinationFileKey = "destination.parquet";

        // Simulate an exception when S3Client tries to fetch the CSV file
        when(s3Client.getObject(any(GetObjectRequest.class))).thenThrow(SdkClientException.class);

        // Perform the test and expect an IOException to be thrown due to S3ClientException
        IOException thrownException = assertThrows(IOException.class, () -> snapshotService.convertCsvToParquetAndUpload(sourceBucketName, sourceFileKey, fileTobeProcessed, destinationBucketName, destinationFileKey));
        assertEquals("Error fetching CSV data from S3", thrownException.getMessage());
    }
}
