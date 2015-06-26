package com.movile.sbs.harvester.jobs;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.movile.sbs.harvester.bean.Record;
import com.movile.sbs.harvester.util.Chronometer;
import com.movile.sbs.harvester.util.FileSorter;

/**
 * @author eitikimura
 */
public final class ExternalSorterJob implements Job {

    private static Logger log = LoggerFactory.getLogger(ExternalSorterJob.class);
    
    private File workDir;
    private File outputDir;
    private Comparator<Record> comparator;
    

    @Override
    public File[] executeJob() throws Exception {

        ExecutorService executor = Executors.newFixedThreadPool(4);
        Chronometer chron = new Chronometer();
        chron.start();

        // get a list of files to process
        List<File> workFiles = getWorkDirFiles();
        CountDownLatch latch = new CountDownLatch(workFiles.size());

        workFiles.stream().forEach((file) -> {
            // creates a new runnable
            Runnable runnable = () -> {
                Chronometer timer = new Chronometer();
                timer.start();
                
                boolean distinct = true; // force remove duplicates
                FileSorter sorter = new FileSorter(file);
                File output = null;
                
                log.info("[SORTER] starting to sort file: {}", file.getPath());
                
                try {
                    // sorts the external file
                    output = sorter.sort(outputDir.getPath(), distinct, comparator);
                    timer.stop();
                } catch (Exception e) {
                    log.error("[SORTER] error sorting file: {} -> {}", file.getPath(), e.getMessage(), e);
                } finally {
                    latch.countDown();
                    log.debug("[SORTER] file: {} processed in {} sec, output file: {}, pending to execute: {}", 
                        file.getName(), timer.getSeconds(), (output != null ? output.getPath() : "ERROR"), latch.getCount());
                    System.gc();
                }
            };
            
            // add runnable to execution
            executor.execute(runnable);
        });

        log.info("[SORTER] waiting tasks to finish, pending: {}", latch.getCount());
        executor.shutdown();
        latch.await(); // wait tasks to finish sorting process

        chron.stop();
        log.info("[SORTER] Job completed in: {} seconds", chron.getSeconds());
        
        return this.outputDir.listFiles();
    }

    /**
     * get a list of workdir files
     * @return the directory files
     */
    private List<File> getWorkDirFiles() {
        return Arrays.asList(workDir.listFiles()).stream()
                .filter(f -> f.isFile()) // filter just files and ignore directories
                .sorted()
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
    public void setComparator(Comparator<Record> comparator) {
        this.comparator = comparator;
    };
    
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