import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;

import java.io.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;
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
    public void testConvertCsvToParquetAndUpload() throws IOException {
        String sourceBucketName = "source-bucket";
        String sourceFileKey = "source-file.csv";
        String fileTobeProcessed = "test-schema";
        String destinationBucketName = "destination-bucket";
        String destinationFileKy = "destination-file.parquet";

        // Mocking readCsvFromS3
        List<String[]> csvData = new ArrayList<>();
        csvData.add(new String[]{"header1", "header2"});
        csvData.add(new String[]{"value1", "value2"});
        doReturn(csvData).when(snapshotService).readCsvFromS3(anyString(), anyString());

        // Mocking loadJsonSchema
        String jsonSchema = "{\"type\": \"record\", \"name\": \"Test\", \"fields\": [{\"name\": \"header1\", \"type\": \"string\"}, {\"name\": \"header2\", \"type\": \"string\"}]}";
        doReturn(jsonSchema).when(snapshotService).loadJsonSchema(anyString());

        // Mocking S3 upload
        doNothing().when(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));

        // Mocking deleteSysParquetFile
        doNothing().when(snapshotService).deleteSysParquetFile(anyString());

        snapshotService.convertCsvToParquetAndUpload(sourceBucketName, sourceFileKey, fileTobeProcessed, destinationBucketName, destinationFileKy);

        verify(snapshotService).readCsvFromS3(sourceBucketName, sourceFileKey);
        verify(snapshotService).loadJsonSchema(fileTobeProcessed);
        verify(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    public void testReadCsvFromS3() throws IOException, CsvException {
        String bucketName = "test-bucket";
        String key = "test-file.csv";

        // Mocking S3 response
        ResponseInputStream<GetObjectResponse> mockResponseInputStream = mock(ResponseInputStream.class);
        when(s3Client.getObject(any(GetObjectRequest.class))).thenReturn(mockResponseInputStream);

        // Mocking CSVReader
        Reader mockReader = mock(Reader.class);
        CSVReader mockCsvReader = mock(CSVReader.class);
        List<String[]> mockCsvData = new ArrayList<>();
        mockCsvData.add(new String[]{"header1", "header2"});
        mockCsvData.add(new String[]{"value1", "value2"});
        when(mockCsvReader.readAll()).thenReturn(mockCsvData);

        // Inject mocks
        ReflectionTestUtils.setField(snapshotService, "csvReader", mockCsvReader);

        List<String[]> result = snapshotService.readCsvFromS3(bucketName, key);

        assertNotNull(result);
        assertEquals(2, result.size());
        verify(s3Client).getObject(any(GetObjectRequest.class));
        verify(mockCsvReader).readAll();
    }

    @Test
    public void testConvertCsvToParquet() throws IOException {
        List<String[]> csvData = new ArrayList<>();
        csvData.add(new String[]{"header1", "header2"});
        csvData.add(new String[]{"value1", "value2"});

        String jsonSchema = "{\"type\": \"record\", \"name\": \"Test\", \"fields\": [{\"name\": \"header1\", \"type\": \"string\"}, {\"name\": \"header2\", \"type\": \"string\"}]}";
        Schema avroSchema = new Schema.Parser().parse(jsonSchema);

        String fileName = "/tmp/test-output.parquet";
        File parquetFile = snapshotService.convertCsvToParquet(csvData, avroSchema, fileName);

        assertTrue(parquetFile.exists());
        assertTrue(parquetFile.length() > 0);
    }

    @Test
    public void testUploadParquetToS3() {
        String destinationBucketName = "destination-bucket";
        String destinationFileKey = "destination-file.parquet";
        File parquetFile = new File("/tmp/test-output.parquet");

        // Mocking S3 upload
        doNothing().when(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));

        snapshotService.uploadParquetToS3(destinationBucketName, destinationFileKey, parquetFile, parquetFile.getName());

        verify(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    public void testDeleteSysParquetFile() {
        String parquetFileName = "/tmp/test-output.parquet";
        File mockFile = mock(File.class);

        when(mockFile.exists()).thenReturn(true);
        when(mockFile.delete()).thenReturn(true);

        snapshotService.deleteSysParquetFile(parquetFileName);

        verify(mockFile).delete();
    }
}
