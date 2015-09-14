package com.movile.sbs.harvester.example;

import java.io.File;
import java.io.IOException;

import com.movile.sbs.harvester.util.FileSorter;

public class SorterProgram {
    
    public static void main(String[] args) throws IOException {
        String path = "log/slice22.log";
        String output = "log/pass-1";
        
        File f = new File(path);
        System.out.println(f.getParentFile().getAbsolutePath());
        
        boolean distinct = true;

        FileSorter sorter = new FileSorter(path);
        File sortedFile = sorter.sort(output, distinct);
        System.out.println(sortedFile.getPath() + "-" + sortedFile.length() + " bytes");
    }
    
}
