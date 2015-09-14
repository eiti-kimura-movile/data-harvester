package com.movile.sbs.harvester.example;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;

public class MergeSort {

    public static void main(String[] args) {

        List<Integer> data1 = Arrays.asList(new Integer[] {9, 2, 1, 3, 10, 1, 0, 6});
        List<Integer> data2 = Arrays.asList(new Integer[] {8, 3, 7, 4, 6, 0, 1, 5, 9});

        List<Integer> dataSet = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            dataSet.add(i);
        }

        // simulate a shuffled data set
        Collections.shuffle(dataSet);
        int finalIndex = dataSet.size();
        int halfIndex = Math.round(finalIndex / 2f);

        List<Integer> page1 = dataSet.subList(0, halfIndex);
        List<Integer> page2 = dataSet.subList(halfIndex, finalIndex);

        System.out.print("raw_data:");
        dataSet.forEach(item -> System.out.print("\t" + item));

        System.out.println("\n\n-- Sorting the dataset --");

        List<Integer> sortedPage1 = sort(page1);
        List<Integer> sortedPage2 = sort(page2);

        System.out.print("\n p1:");
        sortedPage1.forEach(item -> System.out.print("\t" + item));

        System.out.print("\n p2:");
        sortedPage2.forEach(item -> System.out.print("\t" + item));

        List<Integer> merge = merge(sortedPage1, sortedPage2);
        System.out.print("\n merged list:");
        merge.forEach(item -> System.out.print("\t" + item));
    }

    public static List<Integer> sort(List<Integer> page) {
        return page.stream().sorted().distinct().collect(Collectors.toList());
    }

    public static ArrayList<Integer> merge(List<Integer> left, List<Integer> right) {

        ArrayList<Integer> mergedList = new ArrayList<Integer>();
        Logger logFile = createLoggerFor("FILE", "/tmp/result.txt");

        while (left.size() > 0 || right.size() > 0) {
            
            if (left.size() > 0 && right.size() > 0) {
                Integer leftVal = left.get(0);
                Integer rightVal = right.get(0);

                if (leftVal < rightVal) {
                    logFile.info(String.valueOf(leftVal));
                    mergedList.add(leftVal);
                    left.remove(0);
                } else {
                    logFile.info(String.valueOf(rightVal));
                    mergedList.add(rightVal);
                    right.remove(0);
                }

            } else if (left.size() > 0) {
                mergedList.add(left.get(0));
                left.remove(0);

            } else if (right.size() > 0) {
                mergedList.add(right.get(0));
                right.remove(0);
            }
        }

        return mergedList;
    }

    public static List<Integer> asOrderedList(Integer... elements) {
        if (elements == null || elements.length == 0) {
            return null;
        }

        List<Integer> page = Arrays.asList(elements);
        Collections.sort(page);
        return page;
    }

    public static Integer getValue(Iterator<Integer> iterator) {
        return iterator.hasNext() ? iterator.next() : null;
    }

    private static Logger createLoggerFor(String string, String file) {

        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        PatternLayoutEncoder ple = new PatternLayoutEncoder();

        ple.setPattern("%msg%n");
        ple.setContext(lc);
        ple.start();

        FileAppender<ILoggingEvent> fileAppender = new FileAppender<ILoggingEvent>();
        fileAppender.setFile(file);
        fileAppender.setEncoder(ple);
        fileAppender.setContext(lc);
        fileAppender.setAppend(false); //recreate the file each time it run
        fileAppender.start();

        Logger logger = (Logger) LoggerFactory.getLogger(string);
        logger.addAppender(fileAppender);
        logger.setLevel(Level.DEBUG);
        logger.setAdditive(false); /* set to true if root should log too */

        return logger;
    }

}
