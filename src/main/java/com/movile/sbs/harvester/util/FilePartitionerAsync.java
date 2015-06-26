package com.movile.sbs.harvester.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * @author eitikimura
 */
public class FilePartitionerAsync extends Thread {

    private static final long MEGABYTE_IN_BYTES = 1024 * 1024;
    private Long sizeOfFilesInMega;
    private long bytesCounter = 0;

    private File outputDir;
    private String filePrefix;
    private int partCounter = 1;// I like to name parts from 01, 02, 03

    private Writer writer;
    private List<File> chunks;
    
    /**
     * control variable to indicate that object is being destroyed
     */
    private boolean closing = Boolean.FALSE;

    private ArrayBlockingQueue<String> queue;

    public FilePartitionerAsync() {
        this.bytesCounter = 0l;

        this.filePrefix = "part";
        this.sizeOfFilesInMega = 25 * MEGABYTE_IN_BYTES;
        this.outputDir = new File("/tmp");
        this.chunks = new ArrayList<File>();

        queue = new ArrayBlockingQueue<String>(2048);
        start();
    }

    public FilePartitionerAsync(File outputDir, String filePrefix, long sizeOfFilesInMega) {
        this();
        this.outputDir = outputDir;
        this.sizeOfFilesInMega = sizeOfFilesInMega * MEGABYTE_IN_BYTES;
        this.filePrefix = filePrefix;
    }

    @Override
    public void run() {

        while (!this.isInterrupted()) {
            String line = null;

            try {
                line = queue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            try {
                if (line != null && !"".equals(line.trim())) {
                    write(line);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void writeln(String line) throws InterruptedException {
        if (line == null || "".equals(line.trim())) {
            throw new IllegalArgumentException("string line can not be null");
        }

        if (closing) {
           throw new IllegalStateException("this partitioner is closed");    
        }
        
        // add message to queue
        queue.put(line);
    }

    /**
     * write data to a file
     * @param line a string of data
     * @throws IOException
     */
    private void write(String line) throws IOException {

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
    }

    /**
     * @param partCounter
     * @return
     * @throws IOException
     */
    private Writer buildWriter(int partCounter) {
        try {
            if (writer != null) {
                writer.close(); // already flushes
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

        closing = Boolean.TRUE;
        
        // interrupts this thread
        this.interrupt();

        // consume the remaining objects from queue
        queue.stream().forEach(line -> {
            try {
                writeln(writer, line);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // clear the queue
        queue.clear();
        
        if (writer != null) {
            writer.close(); // close already flushes the stream first
        }
        
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
}
