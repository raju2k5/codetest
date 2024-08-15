  @Test
    void testConvertCsvToParquetAndUpload() throws IOException {
        String sourceBucketName = "source-bucket";
        String sourceFileKey = "path/to/source.csv";
        String fileTobeProcessed = "testFileType";
        String destinationBucketName = "destination-bucket";
        String destinationFileKey = "path/to/destination.parquet";

        // Mock the S3 response
        ResponseInputStream<GetObjectResponse> responseInputStream = mock(ResponseInputStream.class);
        when(s3Client.getObject(any(GetObjectRequest.class))).thenReturn(responseInputStream);
        when(responseInputStream.readAllBytes()).thenReturn("header1,header2\nvalue1,value2".getBytes());
        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenReturn(null);

        // Use a hardcoded schema for testing
        String hardcodedSchema = "{\"type\": \"record\", \"name\": \"test\", \"fields\": [{\"name\": \"header1\", \"type\": \"string\"}, {\"name\": \"header2\", \"type\": \"string\"}]}";
        snapshotService.setSchemaForTesting(hardcodedSchema); // New method to set schema

        // Create a temporary file to store Parquet data
        Path tempFilePath = Files.createTempFile("test-output", ".parquet");
        tempFilePath.toFile().deleteOnExit(); // Ensure the file is deleted when the JVM exits

        // Execute the method under test
        assertDoesNotThrow(() -> snapshotService.convertCsvToParquetAndUpload(sourceBucketName, sourceFileKey, fileTobeProcessed, destinationBucketName, tempFilePath.toString()));

        // Verify interactions
        verify(s3Client).getObject(any(GetObjectRequest.class));
        verify(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));

        // Check the record count in the Parquet file
        int recordCount = countRecordsInParquet(tempFilePath.toString());

        // Verify that the record count is as expected
        assertEquals(2, recordCount); // 2 records excluding header
    }

    private int countRecordsInParquet(String parquetFilePath) throws IOException {
        int count = 0;
        try (AvroParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new org.apache.hadoop.fs.Path(parquetFilePath)).build()) {
            while (reader.read() != null) {
                count++;
            }
        }
        return count;
    }
