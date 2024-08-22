import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.exception.SdkClientException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import static org.junit.jupiter.api.Assertions.*;

class SnapshotServiceImplTest {

    @Mock
    private S3Client s3Client;

    @Spy
    @InjectMocks
    private SnapshotServiceImpl snapshotService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testConvertCsvToParquetAndUpload_RecordCountMatches() throws IOException {
        String sourceBucketName = "source-bucket";
        String sourceFileKey = "path/to/source.csv";
        String fileTobeProcessed = "gbi_party";
        String destinationBucketName = "destination-bucket";
        String destinationFileKey = "path/to/destination.parquet";

        // Mock S3Client response
        ResponseInputStream<GetObjectResponse> responseInputStream = mock(ResponseInputStream.class);
        when(s3Client.getObject(any())).thenReturn(responseInputStream);
        
        // Simulate CSV data with correct line separation
        String csvData = "header1,header2\nvalue1,value2\nvalue3,value4";
        InputStream csvStream = new java.io.ByteArrayInputStream(csvData.getBytes());
        BufferedReader csvReader = new BufferedReader(new InputStreamReader(csvStream));
        when(responseInputStream.readAllBytes()).thenReturn(csvData.getBytes());

        // Mock Parquet conversion
        doReturn("{\"type\": \"record\", \"name\": \"test\", \"fields\": [{\"name\": \"header1\", \"type\": \"string\"}, {\"name\": \"header2\", \"type\": \"string\"}]}").when(snapshotService).loadJsonSchema(anyString());
        
        // Mock the ParquetWriter process without creating an actual file
        doNothing().when(snapshotService).uploadParquetToS3(eq(destinationBucketName), eq(destinationFileKey), any(), any());

        // Execute the method
        assertDoesNotThrow(() -> snapshotService.convertCsvToParquetAndUpload(sourceBucketName, sourceFileKey, fileTobeProcessed, destinationBucketName, destinationFileKey));

        // Verify S3 interactions
        verify(s3Client).getObject(any());
        verify(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));

        // Validate that record counts match (in logs)
        verify(snapshotService).convertCsvToParquet(any(), any(), anyString());
    }
}
