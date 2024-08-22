@Test
void testConvertCsvToParquetAndUpload_RecordCount() throws IOException {
    String sourceBucketName = "source-bucket";
    String sourceFileKey = "path/to/source.csv";
    String fileTobeProcessed = "gbi_party";
    String destinationBucketName = "destination-bucket";
    String destinationFileKey = "path/to/destination.parquet";

    // Mock CSV data with a header and two rows
    String csvContent = "header1,header2\nvalue1,value2\nvalue3,value4";
    List<String[]> mockCsvData = Arrays.asList(
        new String[]{"header1", "header2"},
        new String[]{"value1", "value2"},
        new String[]{"value3", "value4"}
    );

    // Mock the S3 client behavior
    ResponseInputStream<GetObjectResponse> mockResponseInputStream = mock(ResponseInputStream.class);
    when(s3Client.getObject(any(GetObjectRequest.class))).thenReturn(mockResponseInputStream);
    when(mockResponseInputStream.readAllBytes()).thenReturn(csvContent.getBytes());

    // Mock the schema loading
    snapshotService = spy(snapshotService);
    doReturn("{\"type\": \"record\", \"name\": \"test\", \"fields\": [{\"name\": \"header1\", \"type\": \"string\"}, {\"name\": \"header2\", \"type\": \"string\"}]}").when(snapshotService).loadJsonSchema(anyString());

    // Mock convertCsvToParquet to return a non-null value without writing a file
    doReturn(new File("mocked-file")).when(snapshotService).convertCsvToParquet(anyList(), any(Schema.class), anyString());

    assertDoesNotThrow(() -> snapshotService.convertCsvToParquetAndUpload(sourceBucketName, sourceFileKey, fileTobeProcessed, destinationBucketName, destinationFileKey));

    // Use ArgumentCaptor to capture the arguments passed to convertCsvToParquet and verify them
    ArgumentCaptor<List> csvDataCaptor = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<Schema> schemaCaptor = ArgumentCaptor.forClass(Schema.class);
    ArgumentCaptor<String> fileNameCaptor = ArgumentCaptor.forClass(String.class);
    
    verify(snapshotService).convertCsvToParquet(csvDataCaptor.capture(), schemaCaptor.capture(), fileNameCaptor.capture());

    // Verify the CSV data passed
    assertEquals(mockCsvData, csvDataCaptor.getValue());
    // Verify that the Parquet conversion was called
    verify(snapshotService).convertCsvToParquet(anyList(), any(Schema.class), anyString());
    // Verify that the S3 upload was attempted
    verify(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
}
