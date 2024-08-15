// Check the record count in the Parquet file
        int recordCount = countRecordsInParquet(tempFilePath);

        // Verify that the record count is as expected
        assertEquals(2, recordCount); // 2 records excluding header
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
