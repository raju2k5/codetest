import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.File;
import java.util.Arrays;
import java.util.List;

@ExtendWith(MockitoExtension.class)
class SnapshotServiceImplTest {

    @Mock
    private S3Client s3Client;

    @Mock
    private ParquetWriter<GenericRecord> parquetWriter;

    @InjectMocks
    private SnapshotServiceImpl snapshotService;

    private final String sourceBucketName = "my-source-bucket";
    private final String sourceFileKey = "gbi/party.csv";
    private final String fileTobeProcessed = "someFileType";
    private final String destinationBucketName = "my-destination-bucket";
    private final String destinationFileKey = "gbi-report/";

    private List<String[]> csvData;
    private Schema avroSchema;

    @BeforeEach
    void setUp() {
        csvData = Arrays.asList(
            new String[]{"header1", "header2"},
            new String[]{"data1", "data2"},
            new String[]{"data3", "data4"}
        );

        avroSchema = new Schema.Parser().parse("{\n" +
                " \"type\": \"record\",\n" +
                " \"name\": \"TestRecord\",\n" +
                " \"fields\": [\n" +
                "   {\"name\": \"header1\", \"type\": \"string\"},\n" +
                "   {\"name\": \"header2\", \"type\": \"string\"}\n" +
                " ]\n" +
                "}");
    }

    @Test
    void testRecordCountMatchBetweenCsvAndParquet() throws Exception {
        // Mock reading CSV data from S3
        SnapshotServiceImpl snapshotServiceSpy = spy(snapshotService);
        doReturn(csvData).when(snapshotServiceSpy).readCsvFromS3(anyString(), anyString());

        // Mock loading JSON schema
        doReturn(avroSchema.toString()).when(snapshotServiceSpy).loadJsonSchema(anyString());

        // Mock Parquet file creation
        doNothing().when(parquetWriter).write(any(GenericRecord.class));

        // Mock the rest of the method to isolate the record count check
        File mockParquetFile = mock(File.class);
        doReturn(mockParquetFile).when(snapshotServiceSpy).convertCsvToParquet(anyList(), any(Schema.class), anyString());

        // Invoke the method under test
        snapshotServiceSpy.convertCsvToParquetAndUpload(sourceBucketName, sourceFileKey, fileTobeProcessed, destinationBucketName, destinationFileKey);

        // Verify that the Parquet writer was called the correct number of times
        int expectedRecordCount = csvData.size() - 1; // minus one for the header row
        verify(parquetWriter, times(expectedRecordCount)).write(any(GenericRecord.class));
    }
}
