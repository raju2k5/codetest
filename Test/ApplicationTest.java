import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import java.util.HashMap;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class ApplicationTest {

    @Mock
    private ConfigurableApplicationContext configurableApplicationContext;

    @Mock
    private Handler handler;

    @InjectMocks
    private Application application;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(configurableApplicationContext.getBean(Handler.class)).thenReturn(handler);
    }

    @Test
    void testApply() {
        Map<String, String> event = new HashMap<>();
        Map<String, Object> expectedResponse = new HashMap<>();
        when(handler.apply(event)).thenReturn(expectedResponse);

        // Mock the static method to return the mocked application context
        try (var mockedContext = mockStatic(Application.class)) {
            mockedContext.when(() -> Application.getapplicationcontext(any(String[].class)))
                         .thenReturn(configurableApplicationContext);

            Map result = application.apply(event);

            assertEquals(expectedResponse, result);
            verify(handler).apply(event);
        }
    }

    @Test
    void testGetApplicationContext() {
        String[] args = {};
        
        // Mock the static method to return the mocked application context
        try (var mockedContext = mockStatic(Application.class)) {
            mockedContext.when(() -> Application.getapplicationcontext(args))
                         .thenReturn(configurableApplicationContext);

            ApplicationContext context = Application.getapplicationcontext(args);

            assertEquals(configurableApplicationContext, context);
        }
    }
}
