import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.core.ResponseInputStream;

import java.io.*;
import java.util.List;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SnapshotServiceImplTest {

    @Mock
    private S3Client s3Client;

    @InjectMocks
    private SnapshotServiceImpl snapshotServiceImpl;

    @BeforeEach
    void setUp() {
        snapshotServiceImpl = new SnapshotServiceImpl(s3Client);
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

        Reader mockReader = new StringReader("header1,header2\nvalue1,value2");
        CSVReader mockCsvReader = new CSVReader(mockReader);
        whenNew(CSVReader.class).withAnyArguments().thenReturn(mockCsvReader);

        // Mock JSON schema loading
        String mockJsonSchema = "{\"type\":\"record\",\"name\":\"test\",\"fields\":[{\"name\":\"header1\",\"type\":\"string\"},{\"name\":\"header2\",\"type\":\"string\"}]}";
        when(snapshotServiceImpl.loadJsonSchema(fileTobeProcessed)).thenReturn(mockJsonSchema);

        // Call the method under test
        snapshotServiceImpl.convertCsvToParquetAndUpload(sourceBucketName, sourceFileKey, fileTobeProcessed, destinationBucketName, destinationFileKey);

        // Verify interactions with S3Client
        verify(s3Client).getObject(any(GetObjectRequest.class));
        verify(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }
}
