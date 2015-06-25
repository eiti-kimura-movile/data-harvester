package com.movile.sbs.harvester.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author eitikimura
 */
public class FilePartitioner {

    private static final long MEGABYTE_IN_BYTES = 1024 * 1024;
    private Long sizeOfFilesInMega;
    private long bytesCounter = 0;

    private File outputDir;
    private String filePrefix;
    private int partCounter = 1;// I like to name parts from 01, 02, 03
    private Writer writer;
    private List<File> chunks;

    public FilePartitioner() {
        this.bytesCounter = 0l;
        
        this.filePrefix = "part";
        this.sizeOfFilesInMega = 25 * MEGABYTE_IN_BYTES;
        this.outputDir = new File("/tmp");
        this.chunks = new ArrayList<File>();

    }

    public FilePartitioner(File outputDir, String filePrefix, long sizeOfFilesInMega) {
        this();
        this.outputDir = outputDir;
        this.sizeOfFilesInMega = sizeOfFilesInMega * MEGABYTE_IN_BYTES;
        this.filePrefix = filePrefix;
    }

    /**
     * write data to a file
     * @param line a string of data
     * @throws IOException
     */
    public synchronized void write(String line) throws IOException {

        if (line == null || "".equals(line.trim())) {
            throw new IllegalArgumentException("string line can not be null");
        }
        
        if (writer == null) {
            prepareOutputDir();
            writer = buildWriter(this.partCounter);
        }

        // accumulating bytes
        bytesCounter += line.getBytes().length;

        if (bytesCounter >= sizeOfFilesInMega) {

            // rotate file
            writer = buildWriter(++partCounter);
            bytesCounter = 0;
        }

        // write data to disk
        writeln(writer, line);
    }

    /**
     * write data to disk
     * @param write the file writer
     * @param line the data line
     * @throws IOException
     */
    private void writeln(Writer write, String line) throws IOException {
       // write data down to the file
        writer.write(line);
        writer.write("\n");
        //writer.flush();
    }
    
    /**
     * @param partCounter
     * @return
     * @throws IOException
     */
    private Writer buildWriter(int partCounter) {
        try {
            if (writer != null) {
                writer.close(); // already flushes it first
            }

            File outputFile = new File(outputDir.getPath() + File.separator + this.filePrefix + "-" + String.format("%02d", partCounter));
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile)));           
            chunks.add(outputFile);
            
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return writer;
    }

    /**
     * close all of the file streams
     * @return a list of chunks for this file
     * @throws IOException
     */
    public List<File> close() throws IOException {
        writer.flush();
        writer.close();
        
        return chunks;
    }

    /**
     * prepares the output dir, create and clean the directory
     */
    private void prepareOutputDir() {
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        } else {
            for (File file : outputDir.listFiles()) {
                file.delete();
            }
        }
    }
    
    /**
     * split a large text file in small chunks, respecting the EOL \n
     * @param file the file to split
     * @param sizeInMegabytes the approximate chunk files size
     * @throws IOException in case of IO and any file manipulation error
     */
    public static void splitTextFile(File file, int sizeInMegabytes) throws IOException {

        int partCounter = 1;// I like to name parts from 01, 02, 03...
        long sizeOfFiles = sizeInMegabytes * (1024 * 1024); // size in MB
        long bytesCounter = 0;

        Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file.getParent() + File.separator + file.getName() + "."
                + String.format("%02d", partCounter++)), "UTF8"));

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {// try-with-resources to ensure closing stream
            String line = null;
            while ((line = reader.readLine()) != null) {
                // accumulating bytes
                bytesCounter += line.getBytes().length;

                if (bytesCounter >= sizeOfFiles) {
                    writer.flush();
                    writer.close();

                    // rotate file
                    writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file.getParent() + File.separator + file.getName() + "."
                            + String.format("%02d", partCounter++)), "UTF8"));
                    bytesCounter = 0;
                }

                // write data down
                writer.write(line);
                writer.write("\n");
            }
        } finally {

            // flush the final file
            writer.flush();
            writer.close();
        }
    }

}
