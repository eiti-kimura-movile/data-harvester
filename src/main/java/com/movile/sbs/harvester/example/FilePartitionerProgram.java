package com.movile.sbs.harvester.example;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;


import com.movile.sbs.harvester.util.FilePartitioner;

public class FilePartitionerProgram {

    public static void main(String[] args) throws IOException {
        String outputDirectory = "partitions/raw";
        FilePartitioner partitioner = new FilePartitioner(new File(outputDirectory), "part", 20);
        
        Files.newBufferedReader(Paths.get("log/dataset.log"))
             .lines()
             .forEach((line) -> {
                 try {
                    // write file to disk
                    partitioner.write(line);
                } catch (IOException e) {
                   throw new RuntimeException(e);
                }
             });
        
        // close the partitioner program
        List<File> files = partitioner.close();
        
        files.stream().forEach((f)-> {
            System.out.println(f.getPath() + "-" + ((f.length() /1024) /1024) + "MB");
        });
    }

}
