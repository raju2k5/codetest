import lombok.extern.slf4j.Slf4j;
import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;

@Slf4j
@Service
public class SnapshotServiceImpl implements SnapshotService {
    private final S3Client s3Client;

    @Autowired
    public SnapshotServiceImpl(final S3Client s3Client) {
        this.s3Client = s3Client;
    }

    @Override
    public void convertCsvToParquetAndUpload(String sourceBucketName, String sourceFileKey, String fileTobeProcessed, String destinationBucketName, String destinationFileKey) throws CsvException, IOException {
        String tempFileName = "/tmp/output_" + System.currentTimeMillis() + ".parquet";
        log.info("Starting streaming CSV to Parquet process. Source: {}/{} -> Destination: {}/{}", 
                 sourceBucketName, sourceFileKey, destinationBucketName, destinationFileKey);

        try {
            // Step 1: Stream CSV data from S3
            List<String[]> csvData = readCsvFromS3(sourceBucketName, sourceFileKey);
            CSVReader csvReader = new CSVReader(new InputStreamReader(s3Client.getObject(GetObjectRequest.builder()
                .bucket(sourceBucketName)
                .key(sourceFileKey)
                .build())));
            
            // Step 2: Set up ParquetWriter
            log.debug("Setting up ParquetWriter for temp file: {}", tempFileName);
            File parquetFile = new File(tempFileName);
            Schema avroSchema = new Schema.Parser().parse(loadJsonSchema(fileTobeProcessed));
            ParquetWriter<GenericRecord> writer = setupParquetWriter(parquetFile, avroSchema);

            // Step 3: Stream through CSV rows
            String[] headers = csvReader.readNext();  // First row as headers
            String currentDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            String currentTimestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));

            String[] row;
            int totalRows = 0;

            while ((row = csvReader.readNext()) != null) {
                GenericRecord avroRecord = createAvroRecord(row, headers, avroSchema, currentDate, currentTimestamp);
                writer.write(avroRecord);  // Write each record to Parquet file
                totalRows++;
            }

            writer.close();  // Close the writer
            log.info("Successfully converted {} rows from CSV to Parquet.", totalRows);

            // Step 4: Upload to S3
            uploadParquetToS3(destinationBucketName, destinationFileKey, parquetFile, tempFileName);

        } catch (SdkClientException | IOException e) {
            log.error("Error during CSV to Parquet conversion and upload: {}", e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public String loadJsonSchema(String fileTobeProcessed) throws IOException {
        String schemaPath = "/schemas/" + fileTobeProcessed + ".json";
        InputStream inputStream = getClass().getResourceAsStream(schemaPath);

        if (inputStream == null) {
            throw new FileNotFoundException("Schema file not found: " + schemaPath);
        }

        return new String(inputStream.readAllBytes());
    }

    @Override
    public List<String[]> readCsvFromS3(String bucketName, String key) throws IOException {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        log.debug("Fetching CSV file from S3 bucket: {}, key: {}", bucketName, key);
        
        try (ResponseInputStream<GetObjectResponse> objectStream = s3Client.getObject(getObjectRequest);
             Reader reader = new InputStreamReader(objectStream);
             CSVReader csvReader = new CSVReader(reader)) {

            List<String[]> csvData = csvReader.readAll();
            log.debug("CSV data successfully read from S3. Number of rows: {}", csvData.size());
            return csvData;
        }
    }

    // Helper method to create Avro record
    private GenericRecord createAvroRecord(String[] row, String[] headers, Schema avroSchema, String currentDate, String currentTimestamp) {
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("EFF_DT", currentDate);
        avroRecord.put("ETL_TS", currentTimestamp);

        // Populate other fields from CSV
        for (Schema.Field field : avroSchema.getFields()) {
            String fieldName = field.name();
            if (!"EFF_DT".equals(fieldName) && !"ETL_TS".equals(fieldName)) {
                int columnIndex = Arrays.asList(headers).indexOf(fieldName);
                avroRecord.put(fieldName, columnIndex >= 0 && columnIndex < row.length ? row[columnIndex] : null);
            }
        }

        return avroRecord;
    }

    @Override
    public File convertCsvToParquet(List<String[]> csvData, Schema avroSchema, String fileName) throws IOException {
        File parquetFile = new File(fileName);

        Configuration hadoopConfig = new Configuration();
        hadoopConfig.set("parquet.native.enabled", "false");

        log.debug("Starting Parquet file writing. Output file: {}", fileName);

        // Set up the Parquet writer
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new Path(parquetFile.getAbsolutePath()))
                .withSchema(avroSchema)
                .withConf(hadoopConfig)
                .withValidation(false)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {

            String currentDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            String currentTimestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));
            String[] headers = csvData.get(0);

            for (String[] record : csvData) {
                GenericRecord avroRecord = createAvroRecord(record, headers, avroSchema, currentDate, currentTimestamp);
                writer.write(avroRecord);
            }

            log.info("Parquet file written successfully. File: {}", fileName);
        }

        return parquetFile;
    }

    // Helper method to set up ParquetWriter
    private ParquetWriter<GenericRecord> setupParquetWriter(File parquetFile, Schema avroSchema) throws IOException {
        return AvroParquetWriter.<GenericRecord>builder(new Path(parquetFile.getAbsolutePath()))
                .withSchema(avroSchema)
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build();
    }

    @Override
    public void uploadParquetToS3(String destinationBucketName, String destinationFileKey, File parquetFile, String tempFileName) {
        log.debug("Uploading Parquet file to S3 bucket: {}, key: {}", destinationBucketName, destinationFileKey);
        try {
            s3Client.putObject(PutObjectRequest.builder()
                    .bucket(destinationBucketName)
                    .key(destinationFileKey)
                    .build(), RequestBody.fromFile(parquetFile));

            log.info("Parquet file uploaded successfully to S3: {}/{}", destinationBucketName, destinationFileKey);
            deleteSysParquetFile(tempFileName);
        } catch (SdkClientException e) {
            log.error("Error uploading Parquet file to S3: {}", e.getMessage(), e);
            throw new RuntimeException("Error uploading Parquet file to S3: " + e.getMessage(), e);
        }
    }

    @Override
    public void deleteSysParquetFile(String parquetFileName) {
        log.debug("Deleting temporary Parquet file: {}", parquetFileName);
        File localParquetFile = new File(parquetFileName);
        File localCrcFile = new File("." + parquetFileName + ".crc");

        if (localParquetFile.exists() && localParquetFile.delete()) {
            log.info("Parquet file deleted successfully: {}", parquetFileName);
        } else {
            log.warn("Parquet file not found or could not be deleted: {}", parquetFileName);
        }

        if (localCrcFile.exists() && localCrcFile.delete()) {
            log.info(".crc file deleted successfully: {}", localCrcFile.getName());
        } else {
            log.warn(".crc file not found or could not be deleted: {}", localCrcFile.getName());
        }
    }
}
