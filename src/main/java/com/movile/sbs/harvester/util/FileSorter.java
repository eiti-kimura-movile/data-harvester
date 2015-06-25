package com.movile.sbs.harvester.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.movile.sbs.harvester.bean.Record;

/**
 * intend to sort small files
 * @author eitikimura
 */
public final class FileSorter {

    private File file;
    
    public FileSorter(String filePath) {
        this.file = new File(filePath);
    }
    
    public FileSorter(File file) {
        this.file = file;
    }
    
    /**
     * sort file
     * @param filePath the input file path to sort
     * @param execute the distinct by object @see equals() and hashcode
     * @return the new sorted File
     * @throws IOException
     */
    public File sort() throws IOException {
        return sort(null, false);
    }

    public File sort(String outputDirPath, boolean distinct) throws IOException {
        return sort(outputDirPath, distinct, null);
    }
    /**
     * sort file
     * @param filePath the input file path to sort
     * @return the new sorted File
     * @throws IOException
     */
    public File sort(String outputDirPath, boolean distinct, Comparator<Record> comparator) throws IOException {

        String outputFilePath;

        if (outputDirPath == null) {
            outputFilePath = file.getPath() + "-sorted";
        } else {
            File outputDir = new File(outputDirPath);
            outputDir.mkdirs();
            
            outputFilePath = outputDirPath + File.separator +  file.getName() + "-sorted";
        }

        Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFilePath), "UTF8"));
        
        // read entire file, map, sort
        List<Record> records = Files.newBufferedReader(Paths.get(file.getPath()))
                .lines()
                .filter(Objects::nonNull)
                .map((line) -> {
                    String att[] = line.split(" ");
                    return new Record(att[0], Long.parseLong(att[1]), Short.valueOf(att[2].trim()), Short.valueOf(att[3]));
                 })
                 .sorted() // natural sort
                 .collect(Collectors.toList());

        // distinct by key
        if (distinct) {
            records = records.stream().distinct().collect(Collectors.toList());
        }
        
        // apply a different comparator
        if (comparator != null) {
            Collections.sort(records, comparator);
        }

        // write file to disk
        records.forEach(rec -> writeToFile(writer, rec.toString()));

        writer.flush();
        writer.close();

        // return the file
        File file = new File(outputFilePath);
        return file;
    }

    /**
     * write a string to a writer
     * @param writer FileWriter
     * @param line the String data
     */
    private void writeToFile(Writer writer, String line) {
        try {
            writer.write(line);
            writer.write("\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
