
        // Create a temporary file to store Parquet data
        File tempFile = File.createTempFile("test-output", ".parquet");
        tempFile.deleteOnExit(); // Ensure the file is deleted when the JVM exits
