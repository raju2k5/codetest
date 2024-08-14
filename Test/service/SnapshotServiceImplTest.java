import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.core.sync.RequestBody;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class SnapshotServiceImplTest {

    @Mock
    private S3Client s3Client;

    @InjectMocks
    private SnapshotServiceImpl snapshotService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testConvertCsvToParquetAndUpload() throws IOException {
        // Prepare CSV data
        List<String[]> csvData = Arrays.asList(
            new String[]{"id", "name"},
            new String[]{"1", "Alice"},
            new String[]{"2", "Bob"}
        );

        // Mock S3 getObject response to return CSV content
        ByteArrayInputStream csvStream = new ByteArrayInputStream("id,name\n1,Alice\n2,Bob\n".getBytes());
        ResponseInputStream<GetObjectResponse> responseInputStream = new ResponseInputStream<>(GetObjectResponse.builder().build(), csvStream);

        // Mock methods
        when(s3Client.getObject(any())).thenReturn(responseInputStream);
        doNothing().when(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        doReturn(csvData).when(snapshotService).readCsvFromS3(anyString(), anyString());

        // Execute the method under test
        snapshotService.convertCsvToParquetAndUpload(
            "source-bucket", "gbi/party.csv", "someFileType", "destination-bucket", "gbi-report/"
        );

        // Verify method calls
        verify(s3Client).getObject(any());
        verify(snapshotService).readCsvFromS3("source-bucket", "gbi/party.csv");
        verify(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    void testConvertCsvToParquet_InvalidCsvHeader() throws IOException {
        // Prepare CSV data with missing header
        List<String[]> csvData = Arrays.asList(
            new String[]{"id", "name"},
            new String[]{"1", "Alice"},
            new String[]{"2", "Bob"}
        );

        // Mock S3 getObject response to return CSV content with missing header
        ByteArrayInputStream csvStream = new ByteArrayInputStream("id,unknownHeader\n1,Alice\n2,Bob\n".getBytes());
        ResponseInputStream<GetObjectResponse> responseInputStream = new ResponseInputStream<>(GetObjectResponse.builder().build(), csvStream);

        // Mock methods
        when(s3Client.getObject(any())).thenReturn(responseInputStream);
        doReturn(csvData).when(snapshotService).readCsvFromS3(anyString(), anyString());

        // Execute the method under test and expect exception
        assertThrows(IOException.class, () -> {
            snapshotService.convertCsvToParquetAndUpload(
                "source-bucket", "gbi/party.csv", "someFileType", "destination-bucket", "gbi-report/"
            );
        });

        // Verify method calls
        verify(s3Client).getObject(any());
        verify(snapshotService).readCsvFromS3("source-bucket", "gbi/party.csv");
    }
}
