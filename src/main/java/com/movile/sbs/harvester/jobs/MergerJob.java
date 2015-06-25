package com.movile.sbs.harvester.jobs;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.movile.sbs.harvester.util.Chronometer;
import com.movile.sbs.harvester.util.FileMerger;

/**
 * @author eitikimura
 */
public final class MergerJob implements Job {

    private static Logger log = LoggerFactory.getLogger(MergerJob.class);

    private File workDir;
    private File outputDir;

    @Override
    public File[] executeJob() throws Exception {

        Chronometer chron = new Chronometer();
        chron.start();

        // get a list of files to process
        List<File> workFiles = getWorkDirFiles().stream()
                .filter(file -> file.isFile()) // files only
                .filter(file -> !file.isHidden()) // not hidden files
                .collect(Collectors.toList());

        // recursive navigate and merge files of dir
        File output = navigateAndMerge(workFiles);
        removeTmpFile(output.getName());
        
        chron.stop();
        log.info("[MERGER] Job completed in: {} seconds - file: {} - {}MB", chron.getSeconds(), output.getName(), output.length() / (1024 * 1024));
        return this.outputDir.listFiles();
    }

    /**
     * remove merge temp files
     */
    private void removeTmpFile(String output) {
        Arrays.asList(outputDir.listFiles()).stream()
              .filter(f -> f.isFile()) //only files
              .filter(f -> !output.equals(f.getName())) // exclude the output result file
              .forEach(f -> f.delete());
    }

    /**
     * recursive navigate and merge directory files
     * @param list a list of valid files to merge
     * @return a merged output file
     * @throws IOException in case of file operation problems
     */
    public File navigateAndMerge(List<File> list) throws IOException {
        
         if (list.size() == 1) {
            return list.remove(0);
         } else {
            File left = list.remove(0);
            File right = navigateAndMerge(list);

            // call merge-sort merge to merge sorted files
            FileMerger merger = new FileMerger(left, right);

            log.info("[MERGER] merging files {} and {}", left.getName(), right.getName());
            
            // execute and get the merged file
            File output = merger.merge(outputDir.getPath());
            return output;
        }
    }
    
    /**
     * get a list of workdir files
     * @return the directory files
     */
    private List<File> getWorkDirFiles() {
        return Arrays.asList(workDir.listFiles()).stream()
                .sorted().collect(Collectors.toList());
    }

    /**
     * validates the work directory
     * @return true if valid, false otherwise
     */
    private boolean isValidWorkDirectory() {
        if (workDir.isDirectory() && workDir.listFiles().length > 0) {
            return true;
        } else {
            return false;
        }
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

    @Override
    public void setWorkDir(String workDirectory) {
        this.workDir = new File(workDirectory.trim());
        if (!isValidWorkDirectory()) {
            throw new IllegalArgumentException("work dir is invalid or is empty");
        }
    }

    @Override
    public void setOutputDir(String outputDirectory) {
        this.outputDir = new File(outputDirectory.trim());
        prepareOutputDir();
    }

}