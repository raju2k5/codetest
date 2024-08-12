
package com.example.demo; // Adjust the package name accordingly

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationContext;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@SpringBootTest
public class ApplicationTest {

    @Mock
    private ApplicationContext applicationContext;

    @Mock
    private Handler handler;

    @InjectMocks
    private Application application;

    @Test
    void testApply() throws Exception {
        // Arrange
        Map<String, String> event = new HashMap<>();
        event.put("sourceBucketName", "bucket-name");
        event.put("sourceFileKey", "file-key");
        event.put("destinationBucketName", "destination-bucket");
        event.put("destinationFileKey", "destination-file");
        event.put("fileTobeProcessed", "file-type");

        when(applicationContext.getBean(Handler.class)).thenReturn(handler);
        when(handler.apply(event)).thenReturn(new HashMap<>());

        // Act
        Map<String, Object> result = application.apply(event);

        // Assert
        verify(applicationContext, times(1)).getBean(Handler.class);
        verify(handler, times(1)).apply(event);
    }
}
