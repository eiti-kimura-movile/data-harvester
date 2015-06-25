package com.movile.sbs.harvester.comparator;

import java.util.Comparator;

import com.movile.sbs.harvester.bean.Record;

/**
 * @author eitikimura
 */
public class RecordComparator implements Comparator<Record> {

    @Override
    public int compare(Record rec1, Record rec2) {

        // 1st order by priority DESC
        if (rec1.getPriority() > rec2.getPriority()) {
            return -1;
        } else if (rec1.getPriority() < rec2.getPriority()) {
            return 1;

        } else {

            // 2nd order by type DESC
            if (rec1.getType() > rec2.getType()) {
                return -1;
            } else if (rec1.getType() < rec2.getType()) {
                return 1;
            } else {

                // 3rd order by timestamp ASC
                if (rec1.getTimestamp() > rec2.getTimestamp()) {
                    return 1;
                } else if (rec1.getTimestamp() < rec2.getTimestamp()) {
                    return -1;
                } else {
                    return 0;
                }
            }
        }
    }

}
