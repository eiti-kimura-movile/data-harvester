package com.movile.sbs.harvester;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FileReaderProgram {

    public static void main(String[] args) throws IOException, InterruptedException {
        long before = System.currentTimeMillis();
        long after = 0;

        long count = Files.newBufferedReader(Paths.get("/home/eitikimura/git-repository/sbs-harvester/log/dataset.log")).lines().count();
        after = System.currentTimeMillis();
        System.out.println(after - before + "ms");
        System.out.println(count);
        
        Thread.sleep(5000);

        System.out.println();
        before = System.currentTimeMillis();
        System.out.println(countLines("/home/eitikimura/git-repository/sbs-harvester/log/dataset.log"));
        after = System.currentTimeMillis();
        System.out.println(after - before + "ms");
        
        Thread.sleep(5000);
        
        System.out.println();
        before = System.currentTimeMillis();
        System.out.println(countLines2("/home/eitikimura/git-repository/sbs-harvester/log/dataset.log"));
        after = System.currentTimeMillis();
        System.out.println(after - before + "ms");

        
        System.out.println("finished");

    }

    public static int countLines(String filename) throws IOException {
        InputStream is = new BufferedInputStream(new FileInputStream(filename));
        try {
            byte[] c = new byte[1024];
            int count = 0;
            int readChars = 0;
            boolean empty = true;
            while ((readChars = is.read(c)) != -1) {
                empty = false;
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n') {
                        ++count;
                    }
                }
            }
            return (count == 0 && !empty) ? 1 : count;
        } finally {
            is.close();
        }
    }

    public static int countLines2(String filename) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(filename));
        try {
            int lines = 0;
            while (reader.readLine() != null)
                lines++;
            return lines;
        } finally {
            reader.close();
        }
    }
}
