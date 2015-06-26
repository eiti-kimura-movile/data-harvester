package com.movile.sbs.harvester.jobs;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
        List<File> workFiles = getWorkingFiles(this.workDir);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        
        int pass = 1;
        
        while (workFiles.size() > 1) {
            Chronometer chronPass = new Chronometer();
            chronPass.start();
            log.info("[MERGE_JOB] starting the pass {}, file(s) to merge: {}", pass, workFiles.size());
            
            List<List<File>> workingBatches = FileMerger.batches(workFiles, 2).collect(Collectors.toList());
            CountDownLatch latch = new CountDownLatch(workingBatches.size());
            
            // breaks in batches of 2 files
            workingBatches.stream().forEach(sublist -> {
                Runnable runnable = () -> {
                    Chronometer chronThread = new Chronometer();
                    chronThread.start();
                    File output = new File(outputDir + File.separator + "_tmp-" + System.nanoTime());
                    try {
                        FileMerger fileMerger = new FileMerger(sublist, output);
                        output = fileMerger.merge();
                    } catch (Exception e) {
                        log.error("Error merging files: {}, e: {}", sublist, e.getCause(), e);
                    } finally {
                        latch.countDown();
                        log.info("[MERGE_THREAD] finished to merge file(s): {}, ouput: {}, time elapsed: {}s", sublist, output, chronThread.getSeconds()); 
                    }
                };
                
                executor.execute(runnable);
            });
            
            latch.await(); // wait tasks to finish merge process
            
            System.gc();
            log.info("[MERGE_JOB] pass-{} finished (time elapsed: {}s)", pass++, chronPass.getSeconds());
            
            //read directory againg
            workFiles = getWorkingFiles(this.outputDir)
                        .stream()
                        //.filter(f -> f.getName().contains("_tmp-")) //just processed files
                        .collect(Collectors.toList());
        }

        executor.shutdown();
        chron.stop();
        log.info("[MERGE_JOB] Job completed in: {} seconds", chron.getSeconds());
        return this.outputDir.listFiles();
    }

    /**
     * get a list of workdir files
     * @return the directory files
     */
    private List<File> getWorkingFiles(File dir) {

        return Arrays.asList(dir.listFiles()).stream()
                .filter(file -> file.isFile()) // files only
                .filter(file -> !file.isHidden()) // not hidden files
                .collect(Collectors.toList());
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