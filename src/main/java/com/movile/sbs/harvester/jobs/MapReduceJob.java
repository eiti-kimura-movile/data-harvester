/**
 * 
 */
package com.movile.sbs.harvester.jobs;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.movile.sbs.harvester.bean.Record;
import com.movile.sbs.harvester.util.Chronometer;
import com.movile.sbs.harvester.util.FilePartitionerAsync;

/**
 * @author J.P.Eiti Kimura (eiti.kimura@movile.com)
 */
public class MapReduceJob implements Job {

    private static Logger log = LoggerFactory.getLogger(MapReduceJob.class);

    private File workDir;
    private File outputDir;
    private Comparator<Record> comparator;

    @Override
    public File[] executeJob() throws Exception {

        log.info("Starting data partitioner job");
        Chronometer genChron = new Chronometer();
        Chronometer opChron = new Chronometer();
        genChron.start();
        opChron.start();

        FilePartitionerAsync partitioner = new FilePartitionerAsync(outputDir, "part", 25);

        // simulate data ingestion to partition
        Files.newBufferedReader(workDir.toPath()).lines().forEach((line) -> {
            try {
                // write file to disk with partition
                partitioner.writeln(line);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
          }
        );

        // close the partitioner program
        List<File> files = partitioner.close();
        files.stream().forEach((f) -> {
            log.info("partition created: {} - {}MB", f.getPath(), ((f.length() / 1024) / 1024));
        });

        log.info("Finished PARTITION job, time elapsed: {}s", opChron.getSeconds());
        opChron.start();

        log.info("Starting Sort Job");

        Job externalSorter = new ExternalSorterJob();
        externalSorter.setWorkDir(partitioner.getOutputDir().getPath());
        externalSorter.setOutputDir(outputDir + File.separator + "pass-1");

        if (comparator != null) {
            externalSorter.setComparator(comparator);
        }

        File[] resultFiles = externalSorter.executeJob();
        Arrays.asList(resultFiles).stream().forEach(file -> log.info(file.getPath()));

        log.info("Finished SORT job, time elapsed: {}s", opChron.getSeconds());
        opChron.start();

        log.info("Starting Merge Job");

        Job fileMergerJob = new MergerJob();
        fileMergerJob.setWorkDir(externalSorter.getOutputDir().getPath());
        fileMergerJob.setOutputDir(outputDir + File.separator + "pass-2");

        File[] mergeResultFiles = fileMergerJob.executeJob();
        Arrays.asList(mergeResultFiles).stream().forEach(file -> log.info(file.getPath()));

        log.info("Finished MERGE job, time elapsed: {}s", opChron.getSeconds());
        log.info("FINISHED. Total time elapsed: {}min ({}s)", genChron.getMinutes(), genChron.getSeconds());

        return mergeResultFiles;
    }

    @Override
    public void setWorkDir(String workDirectory) {
        this.workDir = new File(workDirectory);

    }

    @Override
    public void setOutputDir(String outputDirectory) {
        this.outputDir = new File(outputDirectory);
    }

    @Override
    public File getOutputDir() {
        return this.outputDir;
    }

    @Override
    public void setComparator(Comparator<Record> comparator) {
        this.comparator = comparator;
    }
}
