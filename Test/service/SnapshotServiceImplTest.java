import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Value;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class SnapshotServiceImplTest {

    @Mock
    private S3Client s3Client;

    @Mock
    private ResponseInputStream<GetObjectResponse> responseInputStream;

    @Mock
    private CSVReader csvReader;

    @InjectMocks
    private SnapshotServiceImpl snapshotService;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testConvertCsvToParquetAndUpload() throws Exception {
        // Given
        String sourceBucketName = "source-bucket";
        String sourceFileKey = "gbi/party.csv";
        String fileTobeProcessed = "someFileType";
        String destinationBucketName = "destination-bucket";
        String destinationFileKey = "gbi-report/party.parquet";

        List<String[]> csvData = Arrays.asList(
            new String[]{"header1", "header2"},
            new String[]{"value1", "value2"}
        );
        String jsonSchema = "{\"type\": \"record\", \"name\": \"TestSchema\", \"fields\": [{\"name\": \"header1\", \"type\": \"string\"}, {\"name\": \"header2\", \"type\": \"string\"}]}";

        // Mock S3Client and CSVReader
        when(s3Client.getObject(any(GetObjectRequest.class))).thenReturn(responseInputStream);
        when(csvReader.readAll()).thenReturn(csvData);
        when(snapshotService.loadJsonSchema(fileTobeProcessed)).thenReturn(jsonSchema);

        // Mock Avro schema and Parquet file creation
        Schema avroSchema = new Schema.Parser().parse(jsonSchema);
        File parquetFile = mock(File.class);

        // Simulate successful file upload
        doNothing().when(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));

        // When
        snapshotService.convertCsvToParquetAndUpload(sourceBucketName, sourceFileKey, fileTobeProcessed, destinationBucketName, destinationFileKey);

        // Then
        verify(s3Client).getObject(any(GetObjectRequest.class));
        verify(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    void testConvertCsvToParquet_InvalidCsvHeader() throws Exception {
        // Given
        String sourceBucketName = "source-bucket";
        String sourceFileKey = "gbi/party.csv";
        String fileTobeProcessed = "someFileType";
        String destinationBucketName = "destination-bucket";
        String destinationFileKey = "gbi-report/party.parquet";

        // Mock CSV data with an invalid header
        List<String[]> csvData = Arrays.asList(
            new String[]{"invalidHeader"},
            new String[]{"value1"}
        );
        String jsonSchema = "{\"type\": \"record\", \"name\": \"TestSchema\", \"fields\": [{\"name\": \"header1\", \"type\": \"string\"}]}";

        // Mock S3Client and CSVReader
        when(s3Client.getObject(any(GetObjectRequest.class))).thenReturn(responseInputStream);
        when(csvReader.readAll()).thenReturn(csvData);
        when(snapshotService.loadJsonSchema(fileTobeProcessed)).thenReturn(jsonSchema);

        // When
        Exception exception = assertThrows(IOException.class, () -> {
            snapshotService.convertCsvToParquetAndUpload(sourceBucketName, sourceFileKey, fileTobeProcessed, destinationBucketName, destinationFileKey);
        });

        // Then
        verify(s3Client).getObject(any(GetObjectRequest.class));
        assertTrue(exception.getMessage().contains("CSV header"));
    }
}
