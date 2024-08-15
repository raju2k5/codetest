import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.core.ResponseInputStream;
import com.opencsv.CSVReader;

import java.io.*;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SnapshotServiceImplTest {

    @Mock
    private S3Client s3Client;

    @InjectMocks
    @Spy
    private SnapshotServiceImpl snapshotServiceImpl;

    @BeforeEach
    void setUp() {
        snapshotServiceImpl = Mockito.spy(new SnapshotServiceImpl(s3Client));
    }

    @Test
    void testConvertCsvToParquetAndUpload() throws IOException {
        // Define input parameters
        String sourceBucketName = "source-bucket";
        String sourceFileKey = "source-file.csv";
        String fileTobeProcessed = "fileType";
        String destinationBucketName = "destination-bucket";
        String destinationFileKey = "destination-file.parquet";

        // Mock S3 response for CSV data
        ResponseInputStream<GetObjectResponse> mockResponseInputStream = mock(ResponseInputStream.class);
        when(s3Client.getObject(any(GetObjectRequest.class))).thenReturn(mockResponseInputStream);

        // Mock the reading of CSV data
        Reader mockReader = new StringReader("header1,header2\nvalue1,value2");
        when(mockResponseInputStream.readAllBytes()).thenReturn(mockReader.toString().getBytes());

        // Mock CSVReader
        List<String[]> mockCsvData = Arrays.asList(new String[]{"header1", "header2"}, new String[]{"value1", "value2"});
        CSVReader mockCsvReader = mock(CSVReader.class);
        when(mockCsvReader.readAll()).thenReturn(mockCsvData);

        // Mock the loadJsonSchema method
        String mockJsonSchema = "{\"type\":\"record\",\"name\":\"test\",\"fields\":[{\"name\":\"header1\",\"type\":\"string\"},{\"name\":\"header2\",\"type\":\"string\"}]}";
        doReturn(mockJsonSchema).when(snapshotServiceImpl).loadJsonSchema(fileTobeProcessed);

        // Mock readCsvFromS3 method
        doReturn(mockCsvData).when(snapshotServiceImpl).readCsvFromS3(sourceBucketName, sourceFileKey);

        // Call the method under test
        snapshotServiceImpl.convertCsvToParquetAndUpload(sourceBucketName, sourceFileKey, fileTobeProcessed, destinationBucketName, destinationFileKey);

        // Verify interactions with S3Client
        verify(s3Client).getObject(any(GetObjectRequest.class));
        verify(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }
}
