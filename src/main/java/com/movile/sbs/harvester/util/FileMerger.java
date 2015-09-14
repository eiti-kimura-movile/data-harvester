package com.movile.sbs.harvester.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.movile.sbs.harvester.bean.Record;

/**
 * intend to merge files respecting the sort order
 * @author J.P.Eiti Kimura (eiti.kimura@movile.com)
 */
public final class FileMerger {

    private File leftPart;
    private File rightPart;
    private File outputFile;

    public FileMerger(List<File> files, File outputFile) {
        this(files);
        
        if (outputFile == null) {
            throw new IllegalArgumentException("you need to specify the output file");
        }
        
        this.outputFile = outputFile;
    }

    public FileMerger(List<File> files) {

        if (files == null || files.size() == 0) {
            throw new IllegalArgumentException("you need at least one file to process");
        }

        if (files.size() == 2) {
            this.leftPart = files.get(0);
            this.rightPart = files.get(1);

        } else if (files.size() == 1) {
            this.leftPart = files.get(0);
        } else {
            throw new IllegalArgumentException("can not have more than 2 files to merge");
        }
        
        this.outputFile = new File( this.leftPart.getParent() + File.separator + "_tmp-" + System.nanoTime());
    }

    public FileMerger(File leftPart, File rightPart, File outputFile) {

        if (outputFile == null || (leftPart == null && rightPart == null)) {
            throw new IllegalArgumentException("the file parts and output file can not be null");
        }

        this.leftPart = leftPart;
        this.rightPart = rightPart;
        this.outputFile = outputFile;
    }

    /**
     * sort file
     * @param filePath the input file path to sort
     * @return the new sorted File
     * @throws IOException
     */
    public File merge() throws IOException {

        // nothing to process
        if (leftPart == null && rightPart == null) {
            return null;
        }

        // there is only one file to process
        if (leftPart != null && rightPart == null) {
            // move the left part to the output file
            Files.move(Paths.get(leftPart.getPath()), Paths.get(this.outputFile.getPath()), StandardCopyOption.REPLACE_EXISTING);
            return this.outputFile;
        }

        if (rightPart != null && leftPart == null) {
         // move the left part to the output file
            Files.move(Paths.get(rightPart.getPath()), Paths.get(this.outputFile.getPath()), StandardCopyOption.REPLACE_EXISTING);
            return this.outputFile;
        }

        Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile.getPath()), "UTF8"));

        BufferedReader leftReader = new BufferedReader(new FileReader(leftPart));
        BufferedReader rightReader = new BufferedReader(new FileReader(rightPart));

        Record left = transform(leftReader.readLine());
        Record right = transform(rightReader.readLine());

        // read until we have some data to read from files
        while (left != null || right != null) {
            // merging sorted datasets
            if (left != null && right != null) {
                if (left.compareTo(right) < 0) {
                    writeToFile(writer, left);
                    left = transform(leftReader.readLine());

                } else if (left.compareTo(right) == 0) {
                    // equal data get one of them
                    writeToFile(writer, right);
                    right = transform(rightReader.readLine());

                    // discard the other, do not write it to file
                    left = transform(leftReader.readLine());
                } else {
                    writeToFile(writer, right);
                    right = transform(rightReader.readLine());
                }
            } else if (left != null) {
                writeToFile(writer, left);
                left = transform(leftReader.readLine());

            } else if (right != null) {
                writeToFile(writer, right);
                right = transform(rightReader.readLine());
            }
        }

        leftReader.close();
        rightReader.close();
        writer.close();

        // remove files
        if (leftPart != null) {
            leftPart.delete();
        }
        
        if (rightPart != null) {
            rightPart.delete();
        }

        return outputFile;
    }

    /**
     * split a list into smaller pieces of a given size
     * @param source the list of data
     * @param length the size of batches
     * @return a Stream of batches
     */
    public static <T> Stream<List<T>> batches(List<T> source, int length) {
        if (length <= 0)
            throw new IllegalArgumentException("length = " + length);

        int size = source.size();
        if (size <= 0) {
            return Stream.empty();
        }

        int fullChunks = (size - 1) / length;
        return IntStream.range(0, fullChunks + 1).mapToObj(n -> source.subList(n * length, n == fullChunks ? size : (n + 1) * length));
    }

    public Record transform(String line) {
        if (line != null) {
            String att[] = line.split(" ");
            return new Record(att[0], Long.parseLong(att[1]), Short.valueOf(att[2].trim()), Short.valueOf(att[3]));
        } else {
            return null;
        }
    }

    /**
     * write a string to a writer
     * @param writer FileWriter
     * @param line the String data
     */
    private void writeToFile(Writer writer, Record rec) {
        try {
            if (rec != null) {
                writer.write(rec.toString());
                writer.write("\n");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
