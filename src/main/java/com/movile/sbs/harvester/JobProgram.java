/**
 * 
 */
package com.movile.sbs.harvester;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.movile.sbs.harvester.bean.Record;
import com.movile.sbs.harvester.sqlite.DataDAO;
import com.movile.sbs.harvester.util.Chronometer;

/**
 * @author eitikimura
 *
 */
public class JobProgram {

    private static final int BATCH_SIZE = 10000;
    private static final int REPORT_SIZE = 100000;
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
        
        // creates the data structure
        DataDAO dao = new DataDAO(true);
        log.info("row count: {}", dao.getRowCount());
        
        List<Record> records = new ArrayList<Record>();
        
        AtomicInteger stats = new AtomicInteger();
        //simulate data ingestion to partition
        Files.newBufferedReader(Paths.get("log/data-set-12M.log"))
             .lines()
             .forEach((line) -> {
                 try {
                     String att[] = line.split(" ");
                     Record rec = new Record(att[0], Long.parseLong(att[1]), Short.valueOf(att[2].trim()), Short.valueOf(att[3]));
                     records.add(rec);
                     stats.incrementAndGet();
                     
                     if (records.size() == BATCH_SIZE) {
                         dao.persist(records);
                         records.clear(); 
                     }
                     
                     if (stats.get() == REPORT_SIZE) {
                         stats.set(0);
                         log.info("row count: {}", dao.getRowCount());
                     }
                } catch (Exception e) {
                   throw new RuntimeException(e);
                }
             });
        
        dao.persist(records);
        
        log.info("Finished PERSISTENCE job, time elapsed: {}s", opChron.getSeconds());
        opChron.start();
        
        log.info("Starting Sort Job");
        
        File outputFile = new File("partitions/sorted-output");
        Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile)));           
        
        Statement statement = dao.getStatement();
        String query = "SELECT key, min(timestamp) as mtimestamp, max(type) as mtype, max(priority) as mpriority FROM raw_data GROUP BY key ORDER BY max(priority) DESC, max(type) DESC, min(timestamp) ASC";
        ResultSet rs = statement.executeQuery(query);
  
        while (rs.next()) {
            Record rec = new Record(rs.getString("key"), rs.getLong("mtimestamp"), (short) rs.getInt("mtype"), (short) rs.getInt("mpriority"));
            writer.write(rec.toString());
            writer.write("\n");
            stats.incrementAndGet();
            
            if (stats.get() == REPORT_SIZE) {
                stats.set(0);
                log.info("writen: {} rows", REPORT_SIZE);
            }
        }
        
        writer.close();
        log.info("FINISHED. Total time elapsed: {}min ({}s)", genChron.getMinutes(), genChron.getSeconds());
    }

}
