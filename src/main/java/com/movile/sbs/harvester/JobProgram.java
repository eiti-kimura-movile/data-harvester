/**
 * 
 */
package com.movile.sbs.harvester;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.movile.sbs.harvester.comparator.RecordComparator;
import com.movile.sbs.harvester.jobs.ExternalSorterJob;
import com.movile.sbs.harvester.jobs.Job;
import com.movile.sbs.harvester.jobs.MergerJob;
import com.movile.sbs.harvester.util.Chronometer;
import com.movile.sbs.harvester.util.FilePartitionerAsync;

/**
 * @author eitikimura
 *
 */
public class JobProgram {

    private static Logger log = LoggerFactory.getLogger(JobProgram.class);
    
    /**
     * @param args
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {
        
        log.info("Starting data partitioner job");
        Chronometer genChron = new Chronometer();
        Chronometer opChron = new Chronometer();
        genChron.start();
        opChron.start();
        
        String outputDirectory = "partitions/dataset";
        FilePartitionerAsync partitioner = new FilePartitionerAsync(new File(outputDirectory), "part", 50);
        
        //simulate data ingestion to partition
        Files.newBufferedReader(Paths.get("log/data-set-50M.log"))
             .lines()
             .forEach((line) -> {
                 try {
                    // write file to disk with partition
                    partitioner.writeln(line);
                } catch (Exception e) {
                   throw new RuntimeException(e);
                }
             });
        
        // close the partitioner program
        List<File> files = partitioner.close();
        files.stream().forEach((f)-> {
           log.info("partition created: {} - {}MB", f.getPath(), ((f.length() /1024) /1024));
        });
        
        log.info("Finished PARTITION job, time elapsed: {}s", opChron.getSeconds());
        opChron.start();
        
        log.info("Starting Sort Job");
        
        Job externalSorter = new ExternalSorterJob();
        externalSorter.setWorkDir("partitions/dataset");
        externalSorter.setOutputDir("partitions/dataset/pass-1");
        externalSorter.setComparator(new RecordComparator());
        
        File[] resultFiles = externalSorter.executeJob();
        Arrays.asList(resultFiles)
              .stream()
              .forEach(file -> log.info(file.getPath()));
        
        log.info("Finished SORT job, time elapsed: {}s", opChron.getSeconds());
        opChron.start();
        
        log.info("Starting Merge Job");
        
        Job fileMergerJob = new MergerJob();
        fileMergerJob.setWorkDir("partitions/dataset/pass-1");
        fileMergerJob.setOutputDir("partitions/dataset/pass-2");
        
        resultFiles = fileMergerJob.executeJob();
        Arrays.asList(resultFiles)
              .stream()
              .forEach(file -> log.info(file.getPath()));
        
        log.info("Finished MERGE job, time elapsed: {}s", opChron.getSeconds());
        log.info("FINISHED. Total time elapsed: {}min", genChron.getMinutes());
    }

}
