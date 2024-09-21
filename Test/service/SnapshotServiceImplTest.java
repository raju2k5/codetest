import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.s3.S3Client;
import org.apache.avro.Schema;

import java.io.*;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;

import static org.junit.jupiter.api.Assertions.*;


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
        
        String sourceBucketName = "source-bucket";
        String sourceFileKey = "source.csv";
        String fileTobeProcessed = "gbi_party";
        String destinationBucketName = "destination-bucket";
        String destinationFileKey = "destination.parquet";

        ResponseInputStream<GetObjectResponse> ResponseInputStream = mock(ResponseInputStream.class);

       // Simulate S3Client exception
        when(s3Client.getObject(any(GetObjectRequest.class))).thenReturn(ResponseInputStream);
        when(ResponseInputStream.readAllBytes()).thenReturn("header1, header2\nvalue1, value2".getBytes());
        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenReturn(null);

        //Mock Schema loading
        snapshotService = spy(snapshotService);
        doReturn("{\"type\": \"record\", \"name\": \"test\", \"fields\": [{\"name\": \"header1\", \"type\": \"string\"}, {\"name\": \"header2\", \"type\": \"string\"}]}").when(snapshotService).loadJsonSchema(anyString());

        assertDoesNotThrow(() -> snapshotService.convertCsvToParquetAndUpload(sourceBucketName. sourceFileKey, fileTobeProcessed, destinationBucketName, destinationFileKey));

        //Verify interactions
        verify(s3Client).getObject(any(GetObjectRequest.class));
        verify(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }


    @Test
    void testConvertCsvToParquetAndUpload() throws IOException {
        
        String sourceBucketName = "source-bucket";
        String sourceFileKey = "path/to/source.csv";
        String fileTobeProcessed = "gbi_party";
        String destinationBucketName = "destination-bucket";
        String destinationFileKey = "path/to/destination.parquet";

        ResponseInputStream<GetObjectResponse> ResponseInputStream = mock(ResponseInputStream.class);

       // Simulate S3Client exception
        when(s3Client.getObject(any(GetObjectRequest.class))).thenThrow(SdkClientException.class);

        // Execute the method under test and expect IOException with specific message
        IOException thrownException = assertThrows(IOException.class, () -> snapshotService.convertCsvToParquetAndUpload(sourceBucketName. sourceFileKey, fileTobeProcessed, destinationBucketName, destinationFileKey));
        assertEquals("Error fetching CSV data from S3", thrownException.getMessage());
    }
}
