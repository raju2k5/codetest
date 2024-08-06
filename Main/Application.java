import lombok.SneakyThrows;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import java.util.Map;
import java.util.function.Function;

@SpringBootApplication
@EnableConfigurationProperties

public class Application implements Functions<Map<String, String>, Map>{

    private static ApplicationContext getapplicationcontext(String[] args) {
        return new SpringApplicationBuilder(Application.class).run(args);        
    }
}

public static void main(String[] args) throws Exception {

}

@SneakyThrows
@OVerride
public Map apply(final Map<String, String> event){
    ApplicationContext applicationContext = getapplicationcontext(new String[] {});
    Handler handler = applicationContext.ge;tBean(Handler.class);
    return handler.apply(event);
}
