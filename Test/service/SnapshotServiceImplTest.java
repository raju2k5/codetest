@Test
void testRecordCount() throws IOException {
    String sourceBucketName = "source-bucket";
    String sourceFileKey = "path/to/source.csv";
    String fileTobeProcessed = "testFileType";
    String destinationBucketName = "destination-bucket";
    String destinationFileKey = "path/to/destination.parquet";

    // Mock the CSV data
    ResponseInputStream<GetObjectResponse> responseInputStream = mock(ResponseInputStream.class);
    String csvContent = "header1,header2\nvalue1,value2\nvalue3,value4"; // 3 rows (including header)
    when(s3Client.getObject(any(GetObjectRequest.class))).thenReturn(responseInputStream);
    when(responseInputStream.readAllBytes()).thenReturn(csvContent.getBytes());

    // Mock schema loading
    when(snapshotService.loadJsonSchema(anyString())).thenReturn("{\"type\": \"record\", \"name\": \"test\", \"fields\": [{\"name\": \"header1\", \"type\": \"string\"}, {\"name\": \"header2\", \"type\": \"string\"}]}");

    // Mock the convertCsvToParquet method to avoid file creation
    doAnswer(invocation -> {
        List<String[]> records = invocation.getArgument(0);
        assertEquals(3, records.size()); // Ensure that there are 3 records (including header)
        return null;
    }).when(snapshotService).convertCsvToParquet(anyList(), any(Schema.class), anyString());

    // Execute the method under test
    snapshotService.convertCsvToParquetAndUpload(sourceBucketName, sourceFileKey, fileTobeProcessed, destinationBucketName, destinationFileKey);

    // Verify that convertCsvToParquet was called with the correct number of records
    verify(snapshotService).convertCsvToParquet(anyList(), any(Schema.class), anyString());
}
