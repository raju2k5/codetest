import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationContext;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class ApplicationTest {

    @Mock
    private ApplicationContext applicationContext;

    @Mock
    private Handler handler;

    @InjectMocks
    private Application application;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        // Mock SpringApplicationBuilder to return a mocked ApplicationContext
        SpringApplicationBuilder builder = mock(SpringApplicationBuilder.class);
        when(builder.run(any())).thenReturn(applicationContext);
        when(applicationContext.getBean(Handler.class)).thenReturn(handler);
    }

    @Test
    void testApply() {
        // Prepare input
        Map<String, String> event = new HashMap<>();
        event.put("sourceBucketName", "my-source-bucket");
        event.put("sourceFileKey", "gbi/party.csv");
        event.put("destinationBucketName", "my-destination-bucket");
        event.put("destinationFileKey", "gbi-report/");
        event.put("fileTobeProcessed", "someFileType");

        // Mock handler behavior
        when(handler.apply(event)).thenReturn(new HashMap<>());

        // Call apply method
        Map<String, Object> result = application.apply(event);

        // Verify handler method invocation
        verify(handler).apply(event);

        // Assert the result
        assertEquals(new HashMap<>(), result);
    }

    @Test
    void testGetApplicationContext() {
        // Directly call the method
        ApplicationContext context = Application.getApplicationContext(new String[]{});

        // Verify that it returns a non-null ApplicationContext
        assertEquals(applicationContext, context);
    }
}
