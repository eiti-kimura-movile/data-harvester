/**
 * 
 */
package com.movile.sbs.harvester;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.movile.sbs.harvester.comparator.RecordComparator;
import com.movile.sbs.harvester.jobs.MapReduceJob;
import com.movile.sbs.harvester.util.Chronometer;

/**
 * @author eitikimura
 *
 */
public class JobProgramV2 {

    private static Logger log = LoggerFactory.getLogger(JobProgramV2.class);
    
    /**
     * @param args
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {
        
         Chronometer chron = new Chronometer();
         chron.start();
         
         MapReduceJob distinctJob = new MapReduceJob();
         distinctJob.setWorkDir("log/data-set-100M.log");
         distinctJob.setOutputDir("partitions/distinct");
        
         File[] outputFiles = distinctJob.executeJob();
         
         MapReduceJob sortJob = new MapReduceJob();
         sortJob.setWorkDir(outputFiles[0].getPath());
         sortJob.setOutputDir("partitions/sort");
         sortJob.setComparator(new RecordComparator());
        
         sortJob.executeJob();
         log.info("[FINISHED] total time elapsed: {}min ({}s)", chron.getMinutes(), chron.getSeconds());
    }

}
