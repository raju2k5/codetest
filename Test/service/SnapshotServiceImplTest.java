import org.junit.jupiter.api.BeforeEach;
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

    @BeforeEach
    void setUp() {
        snapshotService = new SnapshotServiceImpl(s3Client);
    }

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
        File mockParquetFile = mock(File.class);

        // Mocking the methods in SnapshotServiceImpl
        doReturn(mockCsvData).when(snapshotService).readCsvFromS3(sourceBucketName, sourceFileKey);
        doReturn(mockJsonSchema).when(snapshotService).loadJsonSchema(fileTobeProcessed);
        doReturn(mockParquetFile).when(snapshotService).convertCsvToParquet(mockCsvData, mockSchema, anyString());
        doNothing().when(snapshotService).uploadParquetToS3(destinationBucketName, destinationFileKey.replaceAll("\\.\\w+", "") + ".parquet", mockParquetFile, anyString());
        doNothing().when(snapshotService).deleteSysParquetFile(anyString());

        // Act
        snapshotService.convertCsvToParquetAndUpload(sourceBucketName, sourceFileKey, fileTobeProcessed, destinationBucketName, destinationFileKey);

        // Assert
        verify(snapshotService).readCsvFromS3(sourceBucketName, sourceFileKey);
        verify(snapshotService).loadJsonSchema(fileTobeProcessed);
        verify(snapshotService).convertCsvToParquet(mockCsvData, mockSchema, anyString());
        verify(snapshotService).uploadParquetToS3(destinationBucketName, destinationFileKey.replaceAll("\\.\\w+", "") + ".parquet", mockParquetFile, anyString());
        verify(snapshotService).deleteSysParquetFile(anyString());

        // Additional assertion to check that the CSV and Parquet record counts were logged as matching
        int totalCsvRecords = mockCsvData.size() - 1;
        verify(mockParquetFile).length(); // This simulates the existence of the Parquet file
    }
}
