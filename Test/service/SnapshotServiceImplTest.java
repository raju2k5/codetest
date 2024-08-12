package com.example.demo; // Adjust the package name accordingly

import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.*;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class SnapshotServiceImplTest {

    @Mock
    private S3Client s3Client;

    @InjectMocks
    private SnapshotServiceImpl snapshotService;

    @BeforeEach
    void setUp() {
        // Setup any necessary initialization before each test
    }

    @Test
    void testConvertCsvToParquetAndUpload() throws Exception {
        // Arrange
        String sourceBucketName = "source-bucket";
        String sourceFileKey = "source-file.csv";
        String fileTobeProcessed = "file-type";
        String destinationBucketName = "destination-bucket";
        String destinationFileKey = "destination-file";

        List<String[]> csvData = Arrays.asList(
            new String[]{"header1", "header2"},
            new String[]{"value1", "value2"}
        );

        when(s3Client.getObject(any()))
                .thenReturn(ResponseInputStream.create(
                    GetObjectResponse.builder().build(), 
                    new ByteArrayInputStream("header1,header2\nvalue1,value2".getBytes())
                ));

        when(snapshotService.loadJsonSchema(anyString())).thenReturn("{\"type\": \"record\", \"name\": \"Test\", \"fields\": [{\"name\": \"header1\", \"type\": \"string\"}, {\"name\": \"header2\", \"type\": \"string\"}]}");

        // Act
        snapshotService.convertCsvToParquetAndUpload(sourceBucketName, sourceFileKey, fileTobeProcessed, destinationBucketName, destinationFileKey);

        // Assert
        verify(s3Client, times(1)).putObject(any(PutObjectRequest.class), any());
    }

    @Test
    void testConvertCsvToParquetAndUpload_SdkClientException() throws Exception {
        // Arrange
        String sourceBucketName = "source-bucket";
        String sourceFileKey = "source-file.csv";
        String fileTobeProcessed = "file-type";
        String destinationBucketName = "destination-bucket";
        String destinationFileKey = "destination-file";

        when(s3Client.getObject(any()))
                .thenThrow(SdkClientException.class);

        // Act & Assert
        assertThrows(IOException.class, () -> snapshotService.convertCsvToParquetAndUpload(
                sourceBucketName, sourceFileKey, fileTobeProcessed, destinationBucketName, destinationFileKey
        ));
    }

    @Test
    void testLoadJsonSchema() throws IOException {
        // Arrange
        String fileTobeProcessed = "test";
        InputStream resourceAsStream = new ByteArrayInputStream("{\"type\": \"record\", \"name\": \"Test\", \"fields\": [{\"name\": \"field1\", \"type\": \"string\"}]}".getBytes());

        when(getClass().getResourceAsStream("/schemas/" + fileTobeProcessed + ".json"))
                .thenReturn(resourceAsStream);

        // Act
        String schema = snapshotService.loadJsonSchema(fileTobeProcessed);

        // Assert
        assertEquals("{\"type\": \"record\", \"name\": \"Test\", \"fields\": [{\"name\": \"field1\", \"type\": \"string\"}]}", schema);
    }

    @Test
    void testLoadJsonSchema_FileNotFound() throws IOException {
        // Arrange
        String fileTobeProcessed = "test";
        when(getClass().getResourceAsStream("/schemas/" + fileTobeProcessed + ".json"))
                .thenReturn(null);

        // Act & Assert
        assertThrows(FileNotFoundException.class, () -> snapshotService.loadJsonSchema(fileTobeProcessed));
    }

    @Test
    void testReadCsvFromS3() throws Exception {
        // Arrange
        String bucketName = "bucket-name";
        String key = "file-key.csv";
        List<String[]> csvData = Arrays.asList(
            new String[]{"header1", "header2"},
            new String[]{"value1", "value2"}
        );

        when(s3Client.getObject(any()))
                .thenReturn(ResponseInputStream.create(
                    GetObjectResponse.builder().build(), 
                    new ByteArrayInputStream("header1,header2\nvalue1,value2".getBytes())
                ));

        // Act
        List<String[]> result = snapshotService.readCsvFromS3(bucketName, key);

        // Assert
        assertEquals(csvData, result);
    }

    @Test
    void testReadCsvFromS3_SdkClientException() throws Exception {
        // Arrange
        String bucketName = "bucket-name";
        String key = "file-key.csv";

        when(s3Client.getObject(any()))
                .thenThrow(SdkClientException.class);

        // Act & Assert
        assertThrows(IOException.class, () -> snapshotService.readCsvFromS3(bucketName, key));
    }

    @Test
    void testConvertCsvToParquet() throws IOException {
        // Arrange
        List<String[]> csvData = Arrays.asList(
            new String[]{"header1", "header2"},
            new String[]{"value1", "value2"}
        );

        Schema schema = new Schema.Parser().parse("{\"type\": \"record\", \"name\": \"Test\", \"fields\": [{\"name\": \"header1\", \"type\": \"string\"}, {\"name\": \"header2\", \"type\": \"string\"}]}");
        String fileName = "/tmp/test.parquet";

        // Act
        File parquetFile = snapshotService.convertCsvToParquet(csvData, schema, fileName);

        // Assert
        assertTrue(parquetFile.exists());
    }

    @Test
    void testUploadParquetToS3() throws Exception {
        // Arrange
        String destinationBucketName = "destination-bucket";
        String destinationFileKey = "destination-file.parquet";
        File parquetFile = new File("/tmp/test.parquet");

        // Act
        snapshotService.uploadParquetToS3(destinationBucketName, destinationFileKey, parquetFile, "/tmp/test.parquet");

        // Assert
        verify(s3Client, times(1)).putObject(any(PutObjectRequest.class), any());
    }

    @Test
    void testDeleteSysParquetFile() {
        // Arrange
        String parquetFileName = "/tmp/test.parquet";

        // Act
        snapshotService.deleteSysParquetFile(parquetFileName);

        // Assert
        // Verify the file is deleted - you might want to mock file deletion in a more advanced setup
    }
}
