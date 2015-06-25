package com.movile.sbs.harvester.util;

import java.util.concurrent.TimeUnit;

/**
 * basic chronometer implementation
 * @author J.P. Eiti Kimura (eiti.kimura@movile.com)
 */
public final class Chronometer {

    private long begin;
    private long end;

    public Chronometer() {
        start();
    }

    public void reset() {
        begin = 0;
        end = 0;
    }

    public void start() {
        begin = System.currentTimeMillis();
        end = 0;
    }

    public void stop() {
        end = System.currentTimeMillis();
    }

    public long getMilliseconds() {
        stop();
        return end - begin;
    }

    public double getSeconds() {
        stop();
        return TimeUnit.MILLISECONDS.toSeconds(end - begin);
    }

    public double getMinutes() {
        stop();
        return TimeUnit.MILLISECONDS.toMinutes(end - begin);
    }

    public double getHours() {
        stop();
        return TimeUnit.MILLISECONDS.toHours(end - begin);
    }
}