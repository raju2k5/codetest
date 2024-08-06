
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.apache.avro.Schema;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class S3ServiceTest {

    @Mock
    private S3Client s3Client;

    @InjectMocks
    private S3Service s3Service;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testConvertCsvToParquetAndUpload_Success() throws IOException, CsvException {
        // Mock the behavior of S3Client and other dependencies
        String bucketName = "test-bucket";
        String fileKey = "test.csv";
        String jsonSchema = "{\"type\":\"record\",\"name\":\"Test\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"},{\"name\":\"field2\",\"type\":\"string\"}]}";

        List<String[]> csvData = Arrays.asList(
                new String[]{"field1", "field2"},
                new String[]{"value1", "value2"}
        );

        ResponseInputStream<GetObjectResponse> mockResponseInputStream = mock(ResponseInputStream.class);
        when(s3Client.getObject(any(GetObjectRequest.class))).thenReturn(mockResponseInputStream);
        when(mockResponseInputStream.read(any(byte[].class))).thenReturn(-1);
        Reader reader = new InputStreamReader(new ByteArrayInputStream(new byte[]{}));
        CSVReader csvReader = new CSVReader(reader);

        // Mock CSV reading
        when(csvReader.readAll()).thenReturn(csvData);

        // Mock Parquet conversion
        File mockParquetFile = mock(File.class);
        when(mockParquetFile.exists()).thenReturn(true);
        when(mockParquetFile.delete()).thenReturn(true);

        // Execute the method to be tested
        s3Service.convertCsvToParquetAndUpload(bucketName, fileKey, jsonSchema);

        // Verify that the necessary methods were called
        verify(s3Client).getObject(any(GetObjectRequest.class));
        verify(s3Client).putObject(any(PutObjectRequest.class), any());
    }

    @Test
    public void testConvertCsvToParquetAndUpload_SdkClientException() throws IOException, CsvException {
        String bucketName = "test-bucket";
        String fileKey = "test.csv";
        String jsonSchema = "{\"type\":\"record\",\"name\":\"Test\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"},{\"name\":\"field2\",\"type\":\"string\"}]}";

        // Mock SdkClientException during S3 getObject call
        when(s3Client.getObject(any(GetObjectRequest.class))).thenThrow(SdkClientException.class);

        // Verify that the SdkClientException is thrown
        assertThrows(SdkClientException.class, () -> {
            s3Service.convertCsvToParquetAndUpload(bucketName, fileKey, jsonSchema);
        });
    }

    @Test
    public void testConvertCsvToParquetAndUpload_CsvException() throws IOException, CsvException {
        String bucketName = "test-bucket";
        String fileKey = "test.csv";
        String jsonSchema = "{\"type\":\"record\",\"name\":\"Test\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"},{\"name\":\"field2\",\"type\":\"string\"}]}";

        // Mock CSVException during CSV reading
        ResponseInputStream<GetObjectResponse> mockResponseInputStream = mock(ResponseInputStream.class);
        when(s3Client.getObject(any(GetObjectRequest.class))).thenReturn(mockResponseInputStream);
        when(mockResponseInputStream.read(any(byte[].class))).thenReturn(-1);
        Reader reader = new InputStreamReader(new ByteArrayInputStream(new byte[]{}));
        CSVReader csvReader = new CSVReader(reader);

        when(csvReader.readAll()).thenThrow(CsvException.class);

        // Verify that the CsvException is thrown
        assertThrows(IOException.class, () -> {
            s3Service.convertCsvToParquetAndUpload(bucketName, fileKey, jsonSchema);
        });
    }

    @Test
    public void testDeleteSysParquetFile_Success() {
        String tempFileName = "tempFile.parquet";

        File mockParquetFile = mock(File.class);
        when(mockParquetFile.exists()).thenReturn(true);
        when(mockParquetFile.delete()).thenReturn(true);

        s3Service.deleteSysParquetFile(tempFileName);

        // Verify that delete method was called
        verify(mockParquetFile, times(1)).delete();
    }

    @Test
    public void testDeleteSysParquetFile_Failure() {
        String tempFileName = "tempFile.parquet";

        File mockParquetFile = mock(File.class);
        when(mockParquetFile.exists()).thenReturn(true);
        when(mockParquetFile.delete()).thenReturn(false);

        s3Service.deleteSysParquetFile(tempFileName);

        // Verify that delete method was called and failed
        verify(mockParquetFile, times(1)).delete();
    }
}
