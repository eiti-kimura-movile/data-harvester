package com.movile.sbs.harvester.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import com.movile.sbs.harvester.bean.Record;

/**
 * intend to merge files respecting the sort order
 * @author eitikimura
 */
public final class FileMerger {

    private File leftPart;
    private File rightPart;

    public FileMerger(File leftPart, File rightPart) {
        this.leftPart = prepareWorkingFile(leftPart);
        this.rightPart = prepareWorkingFile(rightPart);

    }

    private File prepareWorkingFile(File file) {
        // rename and override the previous merged file
        if (file.getName().trim().toLowerCase().contains("merged")) {
            File tmp = new File(file.getParentFile() + File.separator + "_tmp-step-merge");
            tmp.delete();
            file.renameTo(tmp);
            return tmp;
        }
        return file;
    }
    
    /**
     * sort file
     * @param filePath the input file path to sort
     * @return the new sorted File
     * @throws IOException
     */
    public File merge(String outputDirPath) throws IOException {

        File output = new File(outputDirPath + File.separator + "merged-output");
        Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output.getPath()), "UTF8"));

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

        return output;
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
