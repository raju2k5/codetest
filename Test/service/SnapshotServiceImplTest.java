import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import com.opencsv.CSVReader;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.hadoop.conf.Configuration;
import java.io.*;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SnapshotServiceImplTest {

    @Mock
    private S3Client s3Client;

    @InjectMocks
    private SnapshotServiceImpl snapshotService;

    @Mock
    private ResponseInputStream<GetObjectResponse> responseInputStream;

    @Mock
    private CSVReader csvReader;

    @Captor
    private ArgumentCaptor<PutObjectRequest> putObjectRequestCaptor;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testConvertCsvToParquetAndUpload() throws Exception {
        // Given
        String sourceBucketName = "source-bucket";
        String sourceFileKey = "gbi/party.csv";
        String fileTobeProcessed = "someFileType";
        String destinationBucketName = "destination-bucket";
        String destinationFileKey = "gbi-report/";

        List<String[]> csvData = Arrays.asList(
                new String[]{"header1", "header2"},
                new String[]{"data1", "data2"}
        );

        when(s3Client.getObject(any(GetObjectRequest.class))).thenReturn(responseInputStream);
        when(responseInputStream.read(any(byte[].class))).thenReturn(-1); // Simulate end of stream
        when(csvReader.readAll()).thenReturn(csvData);
        doNothing().when(s3Client).putObject(putObjectRequestCaptor.capture(), any(RequestBody.class));

        // Mocking Avro schema and Parquet writer
        String jsonSchema = "{\"type\": \"record\", \"name\": \"TestSchema\", \"fields\": [{\"name\": \"header1\", \"type\": \"string\"}, {\"name\": \"header2\", \"type\": \"string\"}]}";
        Schema avroSchema = new Schema.Parser().parse(jsonSchema);
        when(snapshotService.loadJsonSchema(fileTobeProcessed)).thenReturn(jsonSchema);

        // Mock ParquetWriter
        ParquetWriter<GenericRecord> parquetWriter = mock(ParquetWriter.class);
        doNothing().when(parquetWriter).write(any(GenericRecord.class));

        // When
        snapshotService.convertCsvToParquetAndUpload(sourceBucketName, sourceFileKey, fileTobeProcessed, destinationBucketName, destinationFileKey);

        // Then
        verify(s3Client).getObject(any(GetObjectRequest.class));
        verify(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        assertNotNull(putObjectRequestCaptor.getValue());
        assertEquals("destination-bucket", putObjectRequestCaptor.getValue().bucket());
        assertTrue(putObjectRequestCaptor.getValue().key().endsWith(".parquet"));
    }

    @Test
    void testLoadJsonSchema_FileNotFound() {
        String fileTobeProcessed = "nonExistentFile";
        Exception exception = assertThrows(FileNotFoundException.class, () -> {
            snapshotService.loadJsonSchema(fileTobeProcessed);
        });

        String expectedMessage = "Schema file not found";
        String actualMessage = exception.getMessage();

        assertTrue(actualMessage.contains(expectedMessage));
    }

    @Test
    void testConvertCsvToParquet_InvalidCsvHeader() {
        // Given
        String sourceBucketName = "source-bucket";
        String sourceFileKey = "gbi/party.csv";
        String fileTobeProcessed = "someFileType";
        String destinationBucketName = "destination-bucket";
        String destinationFileKey = "gbi-report/";

        List<String[]> csvData = Arrays.asList(
                new String[]{"wrongHeader1", "wrongHeader2"},
                new String[]{"data1", "data2"}
        );

        when(s3Client.getObject(any(GetObjectRequest.class))).thenReturn(responseInputStream);
        when(responseInputStream.read(any(byte[].class))).thenReturn(-1); // Simulate end of stream
        when(csvReader.readAll()).thenReturn(csvData);
        doNothing().when(s3Client).putObject(putObjectRequestCaptor.capture(), any(RequestBody.class));

        String jsonSchema = "{\"type\": \"record\", \"name\": \"TestSchema\", \"fields\": [{\"name\": \"header1\", \"type\": \"string\"}, {\"name\": \"header2\", \"type\": \"string\"}]}";
        Schema avroSchema = new Schema.Parser().parse(jsonSchema);
        when(snapshotService.loadJsonSchema(fileTobeProcessed)).thenReturn(jsonSchema);

        // When
        Exception exception = assertThrows(IOException.class, () -> {
            snapshotService.convertCsvToParquetAndUpload(sourceBucketName, sourceFileKey, fileTobeProcessed, destinationBucketName, destinationFileKey);
        });

        // Then
        verify(s3Client).getObject(any(GetObjectRequest.class));
        assertTrue(exception.getMessage().contains("CSV header 'header1' not found for Avro field 'header1'"));
    }
}
