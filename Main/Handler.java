import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service

public class Handler {
    private static final String SOURCE_BUCKET_NAME = "sourceBucketName";
    private static final String SOURCE_FILE_KEY = "sourceFileKey";
    private static final String DESTINATION_BUCKET_NAME = "destinationBucketName";
    private static final String DESTINATION_FILE_KEY = "destinationFileKey";
    private static final String PROCESS_FILE_NAME = "fileTobeProcessed";

    @Autowired
    public Handler() {}

    public Map apply(final Map<String, String> event){
        log.info("Received : {}", event);

        event.keyset().forEach(record -> log.info(" Key = {}, Value = {}", record, event.get(record)));

        String sourceBucketName = event.get(SOURCE_BUCKET_NAME);
        String sourceFileKey = event.get(SOURCE_FILE_KEY);
        String destinationBucketName = event.get(DESTINATION_BUCKET_NAME);
        String destinationFileKey = event.get(DESTINATION_FILE_KEY);
        String fileTobeProcessed = event.get(PROCESS_FILE_NAME);


        try{
            snapshotService.convertCsvToParquetAndUpload(sourceBucketName, sourceFileKey, fileTobeProcessed, destinationBucketName, destinationFileKey);
            log.info("Snapshot load completed scuccessfully for => {}", fileTobeProcessed);
        } catch (Exception e){
            throw new RuntimeException("Exception occured during file convertion..")
        }

        final Map<String, Object> response = new HashMap<>();

        return response;
    }
}
