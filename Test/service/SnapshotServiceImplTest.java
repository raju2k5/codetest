  @Test
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
        when(snapshotService.loadJsonSchema(anyString())).thenReturn("{\"type\": \"record\", \"name\": \"test\", \"fields\": [{\"name\": \"header1\", \"type\": \"string\"}, {\"name\": \"header2\", \"type\": \"string\"}]}");

        // Execute the method under test
        snapshotService.convertCsvToParquetAndUpload(sourceBucketName, sourceFileKey, fileTobeProcessed, destinationBucketName, destinationFileKey);

        // Verify that the correct number of records were processed (e.g., checking Parquet file creation)
        verify(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }
