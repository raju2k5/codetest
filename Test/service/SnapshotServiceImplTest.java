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

    // Spy on the convertCsvToParquet method to verify record count indirectly
    SnapshotServiceImpl spyService = spy(snapshotService);
    doNothing().when(spyService).convertCsvToParquet(anyList(), any(Schema.class), anyString());

    // Execute the method under test
    spyService.convertCsvToParquetAndUpload(sourceBucketName, sourceFileKey, fileTobeProcessed, destinationBucketName, destinationFileKey);

    // Verify the interaction with the convertCsvToParquet method
    verify(spyService).convertCsvToParquet(argThat(records -> records.size() == 3), any(Schema.class), anyString());
}
