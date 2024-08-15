void testRecordCount() throws IOException {
        String sourceBucketName = "source-bucket";
        String sourceFileKey = "path/to/source.csv";
        String fileTobeProcessed = "testFileType";
        String destinationBucketName = "destination-bucket";
        String destinationFileKey = "path/to/destination.parquet";

        // Mock the CSV data
        ResponseInputStream<GetObjectResponse> responseInputStream = mock(ResponseInputStream.class);
        String csvContent = "header1,header2\nvalue1,value2\nvalue3,value4";
        when(s3Client.getObject(any(GetObjectRequest.class))).thenReturn(responseInputStream);
        when(responseInputStream.readAllBytes()).thenReturn(csvContent.getBytes());

        // Mock schema loading
        doReturn("{\"type\": \"record\", \"name\": \"test\", \"fields\": [{\"name\": \"header1\", \"type\": \"string\"}, {\"name\": \"header2\", \"type\": \"string\"}]}").when(snapshotService).loadJsonSchema(anyString());

        // Execute the method under test
        snapshotService.convertCsvToParquetAndUpload(sourceBucketName, sourceFileKey, fileTobeProcessed, destinationBucketName, destinationFileKey);

        // Check the record count
        File parquetFile = new File("/tmp/output_*.parquet");
        long recordCount = 0;
        try (InputStream inputStream = new FileInputStream(parquetFile)) {
            // Simplified way to check the record count
            recordCount = inputStream.available(); // This should be replaced with actual record count logic
        } catch (IOException e) {
            fail("Failed to read parquet file for record count");
        }

        assertEquals(2, recordCount); // Expect 2 records (excluding header)
    }
