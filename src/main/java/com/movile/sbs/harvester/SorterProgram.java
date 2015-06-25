package com.movile.sbs.harvester;

import java.io.File;
import java.io.IOException;

import com.movile.sbs.harvester.comparator.RecordComparator;
import com.movile.sbs.harvester.util.FileSorter;

public class SorterProgram {
    
    public static void main(String[] args) throws IOException {
        String path = "partitions/plain-data-slice.log";
        String output = "partitions/pass-1";
        
        File f = new File(path);
        System.out.println(f.getParentFile().getAbsolutePath());
        
        boolean distinct = true;

        FileSorter sorter = new FileSorter(path);
        File sortedFile = sorter.sort(output, distinct, new RecordComparator());
        System.out.println(sortedFile.getPath() + "-" + sortedFile.length() + " bytes");
    }
    
}
