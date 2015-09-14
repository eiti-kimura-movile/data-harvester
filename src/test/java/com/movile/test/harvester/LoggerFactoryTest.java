/**
 * 
 */
package com.movile.test.harvester;

import java.io.File;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;

import com.movile.sbs.harvester.util.LoggerWriterResource;

/**
 * @author J.P.Eiti Kimura (eiti.kimura@movile.com)
 */
public class LoggerFactoryTest {

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Test
    public void simpleLogger() {
        String filePath = System.getProperty("java.io.tmpdir") + "/test-logger/logfile.log";
        Logger logger = LoggerWriterResource.createLoggerFor("test-logger", filePath);
        logger.info("Test this log");
        logger.info("that is a log message");

        File file = new File(filePath);
        Assert.assertTrue(file.exists());
        Assert.assertTrue(file.length() > 0);
        Assert.assertTrue(file.delete());
    }

    @Test
    public void rollingLogger() {
        // just the path witout the filename
        String filePath = System.getProperty("java.io.tmpdir") + "/test-rolling-file-" + System.currentTimeMillis();
        Logger logger = LoggerWriterResource.createFileRollingLoggerFor("test-rolling-logger", filePath, "5KB");

        for (int i = 0; i < 1024; i++) {
            logger.info("{} - This is a log text file, let's test the file roller appender", i);
        }

        File folder = new File(filePath);
        Assert.assertTrue(folder.exists());
        Assert.assertTrue(folder.listFiles().length > 1);
        
        //remove files
        for (File file : folder.listFiles()) {
            file.delete();
        }
 
        //remove dir
        Assert.assertTrue(folder.delete());
    }

}
