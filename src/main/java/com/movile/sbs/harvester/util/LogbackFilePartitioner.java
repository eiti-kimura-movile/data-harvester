package com.movile.sbs.harvester.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import ch.qos.logback.classic.Logger;

/**
 * simple size based file partitioner
 * @author J.P.Eiti Kimura (eiti.kimura@movile.com)
 */
public final class LogbackFilePartitioner {

    public static File[] partition(String filePath) throws IOException {
        String directory = "partitions/" + System.currentTimeMillis();
        return partition(filePath, directory);
    }

    /**
     * partition a big file in small parts
     * @param filePath your file path
     * @return an Array of File
     * @throws IOException
     */
    public static File[] partition(String filePath, String directory) throws IOException {

        Logger partitioner = LoggerWriterResource.createFileRollingLoggerFor("partitioner", directory, "10MB");

        //read entire file
        Files.newBufferedReader(Paths.get(filePath))
             .lines()
             .forEach((string) -> {
                  // write data to partitioner logback appender
                  partitioner.info(string);
              });

        partitioner.detachAndStopAllAppenders();
        File folder = new File(directory);
        return folder.listFiles();
    }

}
