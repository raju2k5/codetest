import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.mockito.Mockito;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
public class SnapshotServiceImplTest {

    @Mock
    private S3Client s3Client;

    @InjectMocks
    private SnapshotServiceImpl snapshotService;

    @Test
    void testConvertCsvToParquetAndUpload() throws Exception {
        // Arrange
        String sourceBucketName = "my-source-bucket";
        String sourceFileKey = "gbi/party.csv";
        String fileTobeProcessed = "someFileType";
        String destinationBucketName = "my-destination-bucket";
        String destinationFileKey = "gbi-report/";

        // Mocking methods
        when(s3Client.getObject(any(GetObjectRequest.class)))
            .thenReturn(ResponseInputStream.create(null));  // Provide a mock response
        when(snapshotService.readCsvFromS3(eq(sourceBucketName), eq(sourceFileKey)))
            .thenReturn(List.of(new String[]{"header1", "header2"}, new String[]{"value1", "value2"}));
        when(snapshotService.loadJsonSchema(eq(fileTobeProcessed)))
            .thenReturn("{ \"type\": \"record\", \"name\": \"TestRecord\", \"fields\": [{\"name\": \"header1\", \"type\": \"string\"}, {\"name\": \"header2\", \"type\": \"string\"}] }");

        // Act
        snapshotService.convertCsvToParquetAndUpload(sourceBucketName, sourceFileKey, fileTobeProcessed, destinationBucketName, destinationFileKey);

        // Verify
        verify(s3Client).putObject(
            any(PutObjectRequest.class),
            any(RequestBody.class)
        );
    }

    @Test
    void testConvertCsvToParquet_InvalidCsvHeader() throws Exception {
        // Arrange
        String sourceBucketName = "my-source-bucket";
        String sourceFileKey = "gbi/party.csv";
        String fileTobeProcessed = "someFileType";
        String destinationBucketName = "my-destination-bucket";
        String destinationFileKey = "gbi-report/";

        // Mocking methods
        when(s3Client.getObject(any(GetObjectRequest.class)))
            .thenReturn(ResponseInputStream.create(null));  // Provide a mock response
        when(snapshotService.readCsvFromS3(eq(sourceBucketName), eq(sourceFileKey)))
            .thenReturn(List.of(new String[]{"header1", "header2"}, new String[]{"value1"}));  // Missing header
        when(snapshotService.loadJsonSchema(eq(fileTobeProcessed)))
            .thenReturn("{ \"type\": \"record\", \"name\": \"TestRecord\", \"fields\": [{\"name\": \"header1\", \"type\": \"string\"}, {\"name\": \"header2\", \"type\": \"string\"}] }");

        // Act & Assert
        Exception exception = assertThrows(IOException.class, () -> {
            snapshotService.convertCsvToParquetAndUpload(sourceBucketName, sourceFileKey, fileTobeProcessed, destinationBucketName, destinationFileKey);
        });

        assertTrue(exception.getMessage().contains("CSV header 'header2' not found for Avro field 'header2'"));
    }
}
