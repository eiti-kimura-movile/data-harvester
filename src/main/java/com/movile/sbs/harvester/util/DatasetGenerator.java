package com.movile.sbs.harvester.util;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author eitikimura
 */
public class DatasetGenerator {

    private static Logger log = LoggerFactory.getLogger("plain");
    private static Random rand = new Random();

    public static void main(String[] args) {

        System.out.println("starting");
        
        long number = 551900000000l;
        long size = 50000000l;

        for (long i = 0; i < size; i++) {
            log.info("{} {} {} {}", (number + i), System.currentTimeMillis(), getNumber(0, 2), getNumber(0, 10));
        }

        System.out.println("finished");
    }

    public static long getNumber(int low, int high) {
        return rand.nextInt(high - low) + low;
    }

}
