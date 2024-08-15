import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.core.exception.SdkClientException;

import java.io.IOException;
import java.util.Collections;

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
    void testConvertCsvToParquetAndUpload_readCsvFromS3IOException() throws IOException {
        String sourceBucketName = "source-bucket";
        String sourceFileKey = "path/to/source.csv";
        String fileTobeProcessed = "testFileType";
        String destinationBucketName = "destination-bucket";
        String destinationFileKey = "path/to/destination.parquet";

        // Simulate IOException in readCsvFromS3 method
        when(s3Client.getObject(any())).thenThrow(new SdkClientException("Simulated S3 exception"));

        // Execute the method under test and expect IOException to be thrown
        IOException thrownException = assertThrows(IOException.class, () -> {
            snapshotService.convertCsvToParquetAndUpload(sourceBucketName, sourceFileKey, fileTobeProcessed, destinationBucketName, destinationFileKey);
        });

        // Verify the exception message
        assertEquals("Error fetching CSV data from S3", thrownException.getMessage());

        // Verify interactions
        verify(s3Client).getObject(any());
    }

    @Test
    void testConvertCsvToParquetAndUpload_readCsvFromS3CsvException() throws IOException {
        String sourceBucketName = "source-bucket";
        String sourceFileKey = "path/to/source.csv";
        String fileTobeProcessed = "testFileType";
        String destinationBucketName = "destination-bucket";
        String destinationFileKey = "path/to/destination.parquet";

        // Simulate successful S3 response but CSV reading throws CsvException
        ResponseInputStream<GetObjectResponse> responseInputStream = mock(ResponseInputStream.class);
        when(s3Client.getObject(any())).thenReturn(responseInputStream);
        when(responseInputStream.readAllBytes()).thenThrow(new IOException("Simulated CSV read error"));

        // Execute the method under test and expect IOException to be thrown
        IOException thrownException = assertThrows(IOException.class, () -> {
            snapshotService.convertCsvToParquetAndUpload(sourceBucketName, sourceFileKey, fileTobeProcessed, destinationBucketName, destinationFileKey);
        });

        // Verify the exception message
        assertEquals("Error reading CSV data from S3", thrownException.getMessage());

        // Verify interactions
        verify(s3Client).getObject(any());
    }
}
