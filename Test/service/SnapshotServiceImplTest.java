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

        // Use a hardcoded schema for testing
        String hardcodedSchema = "{\"type\": \"record\", \"name\": \"test\", \"fields\": [{\"name\": \"header1\", \"type\": \"string\"}, {\"name\": \"header2\", \"type\": \"string\"}]}";
        snapshotService.setSchemaForTesting(hardcodedSchema); // New method to set schema

        // Temporary Parquet file path
        String tempFilePath = "temp_output.parquet";

        // Use the method to write CSV to Parquet
        snapshotService.convertCsvToParquetAndUpload(sourceBucketName, sourceFileKey, fileTobeProcessed, destinationBucketName, destinationFileKey);

        // Check the record count in the Parquet file
        int recordCount = countRecordsInParquet(tempFilePath);

        // Verify that the record count is as expected
        assertEquals(2, recordCount); // 2 records excluding header

        // Clean up
        new File(tempFilePath).delete();
    }

    private int countRecordsInParquet(String parquetFilePath) throws IOException {
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new org.apache.hadoop.fs.Path(parquetFilePath)).build()) {
            int count = 0;
            while (reader.read() != null) {
                count++;
            }
            return count;
        }
    }
}

public void setSchemaForTesting(String schema) {
    this.schema = schema;
}
