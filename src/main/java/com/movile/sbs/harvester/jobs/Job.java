/**
 * 
 */
package com.movile.sbs.harvester.jobs;

import java.io.File;
import java.util.Comparator;

import com.movile.sbs.harvester.bean.Record;

/**
 * @author eitikimura
 *
 */
public interface Job {

    File[] executeJob() throws Exception;
    
    void setWorkDir(String workDirectory);
    
    void setOutputDir(String outputDirectory);
    
    default void setComparator(Comparator<Record> comparator) {
        // do nothing
        throw new NullPointerException("this method is not implemented");
    }
       
}
