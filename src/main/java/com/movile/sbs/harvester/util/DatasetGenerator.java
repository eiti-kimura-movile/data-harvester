package com.movile.sbs.harvester.util;

import java.io.File;
import java.util.Random;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author J.P.Eiti Kimura (eiti.kimura@movile.com)
 */
public class DatasetGenerator {

    private static Logger log = LoggerFactory.getLogger("plain");
    private static Random rand = new Random();

    private static final int DUPLICATE = 10;
    
    public static void main(String[] args) {

        System.out.println("starting");
        
//        long number = 551900000000l;
//        long size = 100000000l;
//
//        int count = 0;
//        for (long i = 0; i < size; i++) {
//            count++;
//            
//            if (count == DUPLICATE) {
//                // duplicate last msisdn
//                log.info("{} {} {} {}", (number + i)-1, System.currentTimeMillis(), getNumber(0, 2), getNumber(0, 10));
//                count = 0;
//            } else {
//                log.info("{} {} {} {}", (number + i), System.currentTimeMillis(), getNumber(0, 2), getNumber(0, 10));
//            }
//        }
        
        generateJson(100000000, 0, null);

        System.out.println("finished");
    }

    private static long getNumber(int low, int high) {
        return rand.nextInt(high - low) + low;
    }
    
    public static void generate(long numRecords, long offset, File outputFile) {

        long number = 551900000000l + offset;
        long size = numRecords;

        int count = 0;
        for (long i = 0; i < size; i++) {
            count++;
            
            if (count == DUPLICATE) {
                // duplicate last msisdn
                log.info("{} {} {} {}", (number + i)-1, System.currentTimeMillis(), getNumber(0, 2), getNumber(0, 10));
                count = 0;
            } else {
                log.info("{} {} {} {}", (number + i), System.currentTimeMillis(), getNumber(0, 2), getNumber(0, 10));
            }
        }
    }
    
    public static void generateJson(long numRecords, long offset, File outputFile) {

        long number = 551900000000l + offset;
        long size = numRecords;

        int count = 0;
        for (long i = 0; i < size; i++) {
            count++;
            
            if (count == DUPLICATE) {
                // duplicate last msisdn
                log.info("{ \"phone\":{}, \"enabled\":{}, \"timeout_date\":{}, \"last_renew_attempt\":{}, \"carrier_id\":{}, \"user_plan\":{}, \"charge_priority\": {}, \"related_id\":\"{}\" }", (number + i)-1, getNumber(0, 2), System.currentTimeMillis(), System.currentTimeMillis() + number, getNumber(1, 6), getNumber(0, 3), getNumber(0, 10), UUID.randomUUID().toString());
                count = 0;
            } else {
                log.info("{ \"phone\":{}, \"enabled\":{}, \"timeout_date\":{}, \"last_renew_attempt\":{}, \"carrier_id\":{}, \"user_plan\":{}, \"charge_priority\": {} }", (number + i)-1, getNumber(0, 2), System.currentTimeMillis(), System.currentTimeMillis() + number, getNumber(1, 6), getNumber(0, 3), getNumber(0, 10));
            }
        }
    }

}
