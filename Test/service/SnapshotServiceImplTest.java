import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.s3.S3Client;
import org.apache.avro.Schema;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class SnapshotServiceImplTest {

    @Mock
    private S3Client s3Client;

    @InjectMocks
    private SnapshotServiceImpl snapshotService;

    private final String sourceBucketName = "source-bucket";
    private final String sourceFileKey = "source.csv";
    private final String fileTobeProcessed = "test-schema";
    private final String destinationBucketName = "destination-bucket";
    private final String destinationFileKey = "destination.parquet";

    @Test
    void testConvertCsvToParquetAndUpload() throws IOException {
        // Arrange
        List<String[]> mockCsvData = Arrays.asList(
                new String[]{"header1", "header2"},  // Header row
                new String[]{"value1", "value2"},
                new String[]{"value3", "value4"}
        );

        String mockJsonSchema = "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[{\"name\":\"header1\",\"type\":\"string\"},{\"name\":\"header2\",\"type\":\"string\"}]}";
        Schema mockSchema = new Schema.Parser().parse(mockJsonSchema);

        // Create a temporary file for the Parquet file
        File mockParquetFile = File.createTempFile("test", ".parquet");
        mockParquetFile.deleteOnExit(); // Ensure it gets deleted after the test

        // Mock the methods in SnapshotServiceImpl
        when(snapshotService.readCsvFromS3(eq(sourceBucketName), eq(sourceFileKey))).thenReturn(mockCsvData);
        when(snapshotService.loadJsonSchema(eq(fileTobeProcessed))).thenReturn(mockJsonSchema);
        when(snapshotService.convertCsvToParquet(eq(mockCsvData), eq(mockSchema), anyString())).thenReturn(mockParquetFile);
        doNothing().when(snapshotService).uploadParquetToS3(eq(destinationBucketName), eq(destinationFileKey.replaceAll("\\.\\w+", "") + ".parquet"), eq(mockParquetFile), anyString());
        doNothing().when(snapshotService).deleteSysParquetFile(anyString());

        // Act
        snapshotService.convertCsvToParquetAndUpload(sourceBucketName, sourceFileKey, fileTobeProcessed, destinationBucketName, destinationFileKey);

        // Assert
        verify(snapshotService).readCsvFromS3(eq(sourceBucketName), eq(sourceFileKey));
        verify(snapshotService).loadJsonSchema(eq(fileTobeProcessed));
        verify(snapshotService).convertCsvToParquet(eq(mockCsvData), eq(mockSchema), anyString());
        verify(snapshotService).uploadParquetToS3(eq(destinationBucketName), eq(destinationFileKey.replaceAll("\\.\\w+", "") + ".parquet"), eq(mockParquetFile), anyString());
        verify(snapshotService).deleteSysParquetFile(anyString());
    }
}
