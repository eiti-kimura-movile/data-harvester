package com.movile.sbs.harvester;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FileReaderProgram {

    public static void main(String[] args) throws IOException, InterruptedException {
        long before = System.currentTimeMillis();
        long after = 0;
        String filename = "log/data-set-50M.log";

        long count = Files.newBufferedReader(Paths.get(filename)).lines().count();
        after = System.currentTimeMillis();
        System.out.println(after - before + "ms");
        System.out.println(count);
        
        Thread.sleep(2000);

        System.out.println();
        before = System.currentTimeMillis();
        System.out.println(countLines(filename));
        after = System.currentTimeMillis();
        System.out.println(after - before + "ms");
        
        Thread.sleep(2000);
        
        System.out.println();
        before = System.currentTimeMillis();
        System.out.println(countLines2(filename));
        after = System.currentTimeMillis();
        System.out.println(after - before + "ms");

        Thread.sleep(2000);
        System.out.println("\nRead With ByteBuffer Byte Array Access");
        before = System.currentTimeMillis();
        readByteBufferByteArrayAccess(filename);
        after = System.currentTimeMillis();
        System.out.println(after - before + "ms");
        
        
        Thread.sleep(2000);
        System.out.println("\nRead With Mapped ByteBuffer");
        before = System.currentTimeMillis();
        readMappedByteBuffer(filename);
        after = System.currentTimeMillis();
        System.out.println(after - before + "ms");
        
        
        Thread.sleep(2000);
        System.out.println("\nRead Fixed Size Buffer");
        before = System.currentTimeMillis();
        readFixedSizeBuffer(filename);
        after = System.currentTimeMillis();
        System.out.println(after - before + "ms");
        
        Thread.sleep(2000);
        System.out.println("\nBufferedReader with Inputstreamreader");
        before = System.currentTimeMillis();
        buffereInputStream(filename);
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
    
    public static void buffereInputStream(String filename) throws IOException {
        InputStream is = new BufferedInputStream(new FileInputStream(filename));
        
        BufferedReader d  = new BufferedReader(new InputStreamReader(is));
        int lines = 0;
        while (d.readLine() != null)
            lines++;
        
        is.close();
        d.close();
        System.out.println("lines : " + lines);
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
    
    public static void readByteBufferByteArrayAccess(String name) throws IOException {
        int SIZE = 8 * 1024; // 8KB
        
        FileInputStream f = new FileInputStream( name );
        FileChannel ch = f.getChannel( );
        byte[] barray = new byte[SIZE];
        ByteBuffer buffer = ByteBuffer.wrap( barray );
        long checkSum = 0L;
        int nRead;
        long lines = 0;
        while ( (nRead=ch.read( buffer )) != -1 )
        {
//            buffer.flip();
//            for (int i = 0; i < buffer.limit(); i++)
//            {
//                char c = (char) buffer.get();
//                System.out.print(c);
//                if ('\n' == c) {
//                    lines++;
//                }
//            }
            
            for ( int i=0; i<nRead; i++ ) {
                checkSum += barray[i];
                
                char c = (char) barray[i];
//                System.out.print(c);
                if ('\n' == c) {
                    lines++;
                }
            }
            
            buffer.clear( );
        }
        
        System.out.println("lines : " + lines + ", checksum: " + checkSum);
    }
    
    
    public static void readFixedSizeBuffer(String name) throws IOException {
        FileInputStream aFile = new FileInputStream( name );
        FileChannel inChannel = aFile.getChannel();
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        long lines = 0;
        
        while(inChannel.read(buffer) > 0)
        {
            buffer.flip();
            for (int i = 0; i < buffer.limit(); i++)
            {
              //System.out.print((char) buffer.get());
                char c = (char) buffer.get();
                if ('\n' == c) {
                    lines++;
                }
            }
            buffer.clear(); // do something with the data and clear/compact it.
        }
        inChannel.close();
        aFile.close();
        System.out.println("lines : " + lines);
    }
    
    public static void readMappedByteBuffer(String name) throws IOException {
        
        FileInputStream aFile = new FileInputStream( name );
        FileChannel inChannel = aFile.getChannel();
        MappedByteBuffer buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, inChannel.size());
        long lines = 0;
        
        buffer.load(); 
        for (int i = 0; i < buffer.limit(); i++)
        {
            //System.out.print((char) buffer.get());
            char c = (char) buffer.get();
            if ('\n' == c) {
                lines++;
            }
        }
        
        buffer.clear(); // do something with the data and clear/compact it.
        inChannel.close();
        aFile.close();
        
        System.out.println("lines : " + lines);
    }
}
