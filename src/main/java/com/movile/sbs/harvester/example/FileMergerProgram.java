package com.movile.sbs.harvester.example;

import java.io.File;
import java.io.IOException;

import com.movile.sbs.harvester.util.FileMerger;

public class FileMergerProgram {

    public static void main(String[] args) throws IOException {
        
        File outputFile =new File("log/pass-1/result.log");
        File leftPart = new File("log/pass-1/slice11.log-sorted");
        File rightPart =  new File("log/pass-1/slice22.log-sorted");
        
        FileMerger merger = new FileMerger(leftPart, rightPart, outputFile);
        merger.merge();
        
        System.out.println("finished");
    }

}
