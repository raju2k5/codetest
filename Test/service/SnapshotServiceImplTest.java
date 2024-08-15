import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class SnapshotServiceImplTest {

    @Mock
    private S3Client s3Client;

    @InjectMocks
    private SnapshotServiceImpl snapshotService;

    @BeforeEach
    public void setUp() {
        snapshotService = new SnapshotServiceImpl(s3Client);
    }

    @Test
    public void testConvertCsvToParquetAndUpload() {
        // Given
        String sourceBucketName = "source-bucket";
        String sourceFileKey = "source-file.csv";
        String fileTobeProcessed = "test-schema";
        String destinationBucketName = "destination-bucket";
        String destinationFileKy = "destination-file.parquet";

        List<String[]> csvData = new ArrayList<>();
        csvData.add(new String[]{"header1", "header2"});
        csvData.add(new String[]{"value1", "value2"});

        String jsonSchema = "{\"type\": \"record\", \"name\": \"Test\", \"fields\": [{\"name\": \"header1\", \"type\": \"string\"}, {\"name\": \"header2\", \"type\": \"string\"}]}";

        File mockParquetFile = new File("/tmp/output.parquet");

        try {
            // When
            // Mocking methods
            doReturn(csvData).when(snapshotService).readCsvFromS3(sourceBucketName, sourceFileKey);
            doReturn(jsonSchema).when(snapshotService).loadJsonSchema(fileTobeProcessed);
            doReturn(mockParquetFile).when(snapshotService).convertCsvToParquet(any(List.class), any(), anyString());
            doNothing().when(snapshotService).uploadParquetToS3(anyString(), anyString(), any(File.class), anyString());

            // Then
            // Run the method under test
            assertDoesNotThrow(() -> snapshotService.convertCsvToParquetAndUpload(
                    sourceBucketName, sourceFileKey, fileTobeProcessed, destinationBucketName, destinationFileKy
            ));

            // Verify the interactions
            verify(snapshotService).readCsvFromS3(sourceBucketName, sourceFileKey);
            verify(snapshotService).loadJsonSchema(fileTobeProcessed);
            verify(snapshotService).convertCsvToParquet(any(List.class), any(), anyString());
            verify(snapshotService).uploadParquetToS3(destinationBucketName, destinationFileKy.replaceAll("\\.\\w+", "") + ".parquet", mockParquetFile, mockParquetFile.getAbsolutePath());
        } catch (IOException e) {
            // Handle any unexpected IOException
            e.printStackTrace();
        }
    }
}
