package com.example.demo; // Adjust the package name accordingly

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class HandlerTest {

    @Mock
    private SnapshotService snapshotService;

    @InjectMocks
    private Handler handler;

    @Test
    void testApply() throws Exception {
        // Arrange
        Map<String, String> event = new HashMap<>();
        event.put("sourceBucketName", "bucket-name");
        event.put("sourceFileKey", "file-key");
        event.put("destinationBucketName", "destination-bucket");
        event.put("destinationFileKey", "destination-file");
        event.put("fileTobeProcessed", "file-type");

        // Act
        Map<String, Object> result = handler.apply(event);

        // Assert
        verify(snapshotService, times(1)).convertCsvToParquetAndUpload(
            "bucket-name",
            "file-key",
            "file-type",
            "destination-bucket",
            "destination-file.parquet"
        );
    }

    @Test
    void testApply_Exception() {
        // Arrange
        Map<String, String> event = new HashMap<>();
        event.put("sourceBucketName", "bucket-name");
        event.put("sourceFileKey", "file-key");
        event.put("destinationBucketName", "destination-bucket");
        event.put("destinationFileKey", "destination-file");
        event.put("fileTobeProcessed", "file-type");

        doThrow(new RuntimeException("Exception occured during file conversion.."))
            .when(snapshotService).convertCsvToParquetAndUpload(
                "bucket-name",
                "file-key",
                "file-type",
                "destination-bucket",
                "destination-file.parquet"
            );

        // Act & Assert
        assertThrows(RuntimeException.class, () -> handler.apply(event));
    }
}

