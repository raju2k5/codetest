import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.hadoop.conf.Configuration;

import java.io.*;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SnapshotServiceImplTest {

    @Mock
    private S3Client s3Client;

    @InjectMocks
    private SnapshotServiceImpl snapshotService;

    private final String sourceBucketName = "source-bucket";
    private final String sourceFileKey = "source-file.csv";
    private final String fileTobeProcessed = "test-schema";
    private final String destinationBucketName = "destination-bucket";
    private final String destinationFileKey = "destination-file.parquet";

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testConvertCsvToParquetAndUpload_success() throws Exception {
        // Mock CSV data
        List<String[]> csvData = Arrays.asList(
                new String[]{"header1", "header2"},
                new String[]{"value1", "value2"}
        );

        // Mock JSON schema
        String jsonSchema = "{ \"type\": \"record\", \"name\": \"TestSchema\", \"fields\": [{\"name\": \"header1\", \"type\": \"string\"}, {\"name\": \"header2\", \"type\": \"string\"}] }";
        Schema avroSchema = new Schema.Parser().parse(jsonSchema);

        // Mock S3 getObject
        ResponseInputStream<GetObjectResponse> mockResponseInputStream = mock(ResponseInputStream.class);
        when(s3Client.getObject(any())).thenReturn(mockResponseInputStream);

        // Mock CSVReader
        Reader mockReader = mock(Reader.class);
        CSVReader mockCsvReader = mock(CSVReader.class);
        when(mockCsvReader.readAll()).thenReturn(csvData);
        whenNew(CSVReader.class).withArguments(any(Reader.class)).thenReturn(mockCsvReader);

        // Mock ParquetWriter
        ParquetWriter<GenericRecord> mockParquetWriter = mock(ParquetWriter.class);
        whenNew(ParquetWriter.class).withAnyArguments().thenReturn(mockParquetWriter);

        // Mock File deletion
        File mockFile = mock(File.class);
        when(mockFile.delete()).thenReturn(true);
        whenNew(File.class).withAnyArguments().thenReturn(mockFile);

        // Execute method
        snapshotService.convertCsvToParquetAndUpload(sourceBucketName, sourceFileKey, fileTobeProcessed, destinationBucketName, destinationFileKey);

        // Verify S3 putObject was called
        verify(s3Client, times(1)).putObject(any(PutObjectRequest.class), any(RequestBody.class));

        // Verify CSVReader and ParquetWriter were used
        verify(mockCsvReader, times(1)).readAll();
        verify(mockParquetWriter, times(csvData.size() - 1)).write(any(GenericRecord.class));
    }

    @Test
    void testConvertCsvToParquetAndUpload_awsSdkException() throws Exception {
        // Mock S3 getObject to throw SdkClientException
        when(s3Client.getObject(any())).thenThrow(SdkClientException.class);

        // Execute method and verify exception
        assertThrows(SdkClientException.class, () ->
                snapshotService.convertCsvToParquetAndUpload(sourceBucketName, sourceFileKey, fileTobeProcessed, destinationBucketName, destinationFileKey)
        );

        // Verify S3 putObject was never called
        verify(s3Client, never()).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    void testConvertCsvToParquetAndUpload_ioException() throws Exception {
        // Mock CSVReader to throw IOException
        ResponseInputStream<GetObjectResponse> mockResponseInputStream = mock(ResponseInputStream.class);
        when(s3Client.getObject(any())).thenReturn(mockResponseInputStream);

        Reader mockReader = mock(Reader.class);
        CSVReader mockCsvReader = mock(CSVReader.class);
        when(mockCsvReader.readAll()).thenThrow(IOException.class);
        whenNew(CSVReader.class).withArguments(any(Reader.class)).thenReturn(mockCsvReader);

        // Execute method and verify exception
        assertThrows(IOException.class, () ->
                snapshotService.convertCsvToParquetAndUpload(sourceBucketName, sourceFileKey, fileTobeProcessed, destinationBucketName, destinationFileKey)
        );

        // Verify S3 putObject was never called
        verify(s3Client, never()).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }
}
