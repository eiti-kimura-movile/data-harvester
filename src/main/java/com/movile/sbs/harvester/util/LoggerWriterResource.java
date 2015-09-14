package com.movile.sbs.harvester.util;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;
import ch.qos.logback.core.rolling.FixedWindowRollingPolicy;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.SizeBasedTriggeringPolicy;
import ch.qos.logback.core.util.StatusPrinter;

/**
 * dynamically create logback file appenders
 * @author J.P.Eiti Kimura (eiti.kimura@movile.com)
 */
public final class LoggerWriterResource {

    /**
     * creates rolling file appender and a new logger for a plain file
     * @param name the logger name
     * @param path the file path
     * @return the Logback logger
     */
    public static Logger createFileRollingLoggerFor(String name, String path) {
        return createFileRollingLoggerFor(name, path, "25MB");
    }
    
    /**
     * creates a simple new file appender a new logger for a plain file
     * @param name the logger name
     * @param filePath the complete file path
     * @return a brand new Logback Logger object
     */
    public static Logger createLoggerFor(String name, String filePath) {
        
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        PatternLayoutEncoder ple = new PatternLayoutEncoder();

        ple.setPattern("%msg%n");
        ple.setContext(lc);
        ple.start();

        FileAppender<ILoggingEvent> fileAppender = new FileAppender<ILoggingEvent>();
        fileAppender.setName(name + "-appender");
        fileAppender.setFile(filePath);
        fileAppender.setEncoder(ple);
        fileAppender.setContext(lc);
        fileAppender.setAppend(false); //recreate the file each time it runs
        fileAppender.start();

        Logger logger = (Logger) LoggerFactory.getLogger(name);
        logger.addAppender(fileAppender);
        logger.setLevel(Level.INFO);
        logger.setAdditive(false); /* set to true if root should log too */

        // OPTIONAL: print logback internal status messages
        StatusPrinter.print(lc);
        
        return logger;
    }

    /**
     * creates rolling file appender and a new logger for a plain file
     * @param name the logger name
     * @param path the file path
     * @param maxFileSize the max file size for rolling
     * @return the Logback logger
     */
    public static Logger createFileRollingLoggerFor(String name, String path, String maxFileSize) {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();

        RollingFileAppender<ILoggingEvent>  rfAppender = new RollingFileAppender<ILoggingEvent>();
        rfAppender.setContext(loggerContext);
        rfAppender.setName(name + "-appender");
        rfAppender.setFile(path + "/part-00");
        
        FixedWindowRollingPolicy rollingPolicy = new FixedWindowRollingPolicy();
        rollingPolicy.setContext(loggerContext);
        // rolling policies need to know their parent
        // it's one of the rare cases, where a sub-component knows about its parent
        rollingPolicy.setParent(rfAppender);
        rollingPolicy.setFileNamePattern(path + "/part-0%i");
        rollingPolicy.start();

        SizeBasedTriggeringPolicy<ILoggingEvent>  triggeringPolicy = new ch.qos.logback.core.rolling.SizeBasedTriggeringPolicy<ILoggingEvent>();
        triggeringPolicy.setMaxFileSize(maxFileSize);
        triggeringPolicy.setContext(loggerContext);
        triggeringPolicy.start();

        PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setContext(loggerContext);
        encoder.setPattern("%msg%n");
        encoder.start();

        rfAppender.setEncoder(encoder);
        rfAppender.setRollingPolicy(rollingPolicy);
        rfAppender.setTriggeringPolicy(triggeringPolicy);
        rfAppender.start();

        // attach the rolling file appender to the logger of your choice
        Logger logbackLogger = loggerContext.getLogger(name);
        logbackLogger.addAppender(rfAppender);
        logbackLogger.setLevel(Level.INFO);
        logbackLogger.setAdditive(false); /* set to true if root should log too */

        // OPTIONAL: print logback internal status messages
        StatusPrinter.print(loggerContext);
        return logbackLogger;
    }
    
}
