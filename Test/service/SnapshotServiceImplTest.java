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
        when(s3Client.putObject(any(), any(RequestBody.class))).thenReturn(null);

        // Mock the schema loading to return a hardcoded schema
        when(snapshotService.loadJsonSchema(anyString())).thenReturn(
            "{\"type\": \"record\", \"name\": \"test\", \"fields\": [{\"name\": \"header1\", \"type\": \"string\"}, {\"name\": \"header2\", \"type\": \"string\"}]}"
        );

        // Create a temporary file to store Parquet data
        java.nio.file.Path tempFilePath = Files.createTempFile("test-output", ".parquet");
        tempFilePath.toFile().deleteOnExit(); // Ensure the file is deleted when the JVM exits

        // Execute the method under test
        assertDoesNotThrow(() -> snapshotService.convertCsvToParquetAndUpload(sourceBucketName, sourceFileKey, fileTobeProcessed, destinationBucketName, tempFilePath.toString()));

        // Verify interactions
        verify(s3Client).getObject(any(GetObjectRequest.class));
        verify(s3Client).putObject(any(), any(RequestBody.class));

        // Check the record count in the Parquet file
        int recordCount = countRecordsInParquet(tempFilePath);

        // Verify that the record count is as expected
        assertEquals(1, recordCount); // 1 record in this case
    }

    private int countRecordsInParquet(java.nio.file.Path parquetFilePath) throws IOException {
        int count = 0;
        Configuration conf = new Configuration();
        Path hadoopPath = new Path(parquetFilePath.toUri()); // Convert to Hadoop Path

        try (ParquetReader<GenericRecord> reader = ParquetReader.builder(new AvroReadSupport<>(), hadoopPath)
                .withConf(conf)
                .build()) {
            while (reader.read() != null) {
                count++;
            }
        }
        return count;
    }
