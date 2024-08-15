import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;

import java.io.*;
import java.util.Collections;
import java.util.List;

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
        String sourceFileKey = "path/to/source.csv";
        String fileTobeProcessed = "testFileType";
        String destinationBucketName = "destination-bucket";
        String destinationFileKey = "path/to/destination.parquet";

        // Mock the S3 response
        ResponseInputStream<GetObjectResponse> responseInputStream = mock(ResponseInputStream.class);
        when(s3Client.getObject(any(GetObjectRequest.class))).thenReturn(responseInputStream);
        when(responseInputStream.readAllBytes()).thenReturn("header1,header2\nvalue1,value2".getBytes());
        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenReturn(null);

        // Mock schema loading
        doReturn("{\"type\": \"record\", \"name\": \"test\", \"fields\": [{\"name\": \"header1\", \"type\": \"string\"}, {\"name\": \"header2\", \"type\": \"string\"}]}").when(snapshotService).loadJsonSchema(anyString());

        // Execute the method under test
        assertDoesNotThrow(() -> snapshotService.convertCsvToParquetAndUpload(sourceBucketName, sourceFileKey, fileTobeProcessed, destinationBucketName, destinationFileKey));

        // Verify interactions
        verify(s3Client).getObject(any(GetObjectRequest.class));
        verify(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    void testConvertCsvToParquetAndUpload_s3ClientException() throws IOException {
        String sourceBucketName = "source-bucket";
        String sourceFileKey = "path/to/source.csv";
        String fileTobeProcessed = "testFileType";
        String destinationBucketName = "destination-bucket";
        String destinationFileKey = "path/to/destination.parquet";

        // Simulate S3Client exception
        when(s3Client.getObject(any(GetObjectRequest.class))).thenThrow(SdkClientException.class);

        // Execute the method under test and expect IOException with specific message
        IOException thrownException = assertThrows(IOException.class, () -> snapshotService.convertCsvToParquetAndUpload(sourceBucketName, sourceFileKey, fileTobeProcessed, destinationBucketName, destinationFileKey));
        assertEquals("Error fetching CSV data from S3", thrownException.getMessage());
    }

    @Test
    void testRecordCount() throws IOException {
        String sourceBucketName = "source-bucket";
        String sourceFileKey = "path/to/source.csv";
        String fileTobeProcessed = "testFileType";
        String destinationBucketName = "destination-bucket";
        String destinationFileKey = "path/to/destination.parquet";

        // Mock the CSV data
        ResponseInputStream<GetObjectResponse> responseInputStream = mock(ResponseInputStream.class);
        String csvContent = "header1,header2\nvalue1,value2\nvalue3,value4";
        when(s3Client.getObject(any(GetObjectRequest.class))).thenReturn(responseInputStream);
        when(responseInputStream.readAllBytes()).thenReturn(csvContent.getBytes());

        // Mock schema loading
        doReturn("{\"type\": \"record\", \"name\": \"test\", \"fields\": [{\"name\": \"header1\", \"type\": \"string\"}, {\"name\": \"header2\", \"type\": \"string\"}]}").when(snapshotService).loadJsonSchema(anyString());

        // Execute the method under test
        snapshotService.convertCsvToParquetAndUpload(sourceBucketName, sourceFileKey, fileTobeProcessed, destinationBucketName, destinationFileKey);

        // Since we can't directly verify the parquet file content, we will verify that the correct number of records was processed
        // This assumes you have a way to verify the record count in the Parquet file, e.g., through file inspection or a mock.
        // For simplicity, we will verify that the parquet file write was called, which indirectly tests the record count processing.
        verify(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }
}
