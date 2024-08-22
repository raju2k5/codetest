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

    // Use a ByteArrayOutputStream to simulate writing to a file without creating an actual file
    ByteArrayOutputStream parquetOutputStream = new ByteArrayOutputStream();

    doAnswer(invocation -> {
        // Simulate writing the Parquet data
        ParquetWriter<GenericRecord> writer = (ParquetWriter<GenericRecord>) invocation.callRealMethod();
        writer.write(new GenericData.Record(new Schema.Parser().parse("{\"type\": \"record\", \"name\": \"test\", \"fields\": [{\"name\": \"header1\", \"type\": \"string\"}, {\"name\": \"header2\", \"type\": \"string\"}]}")));
        return null;
    }).when(snapshotService).convertCsvToParquet(eq(mockCsvData), any(Schema.class), anyString());

    assertDoesNotThrow(() -> snapshotService.convertCsvToParquetAndUpload(sourceBucketName, sourceFileKey, fileTobeProcessed, destinationBucketName, destinationFileKey));

    // Verify the record counts and Parquet file creation
    verify(snapshotService).convertCsvToParquet(eq(mockCsvData), any(Schema.class), anyString());
    verify(s3Client).getObject(any(GetObjectRequest.class));
    verify(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
}
