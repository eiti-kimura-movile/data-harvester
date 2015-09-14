package com.movile.sbs.harvester.bean;

/**
 * Any record representation
 * @author J.P.Eiti Kimura (eiti.kimura@movile.com)
 */
public class Record implements Comparable<Record> {

    private String key;
    private Long timestamp;
    private Short type; //may be 1 for POS and PRE in other case
    private Short priority;

    public Record(String key, Long timestamp) {
        super();
        this.key = key;
        this.timestamp = timestamp;
    }

    public Record(String key, Long timestamp, Short type, Short priority) {
        super();
        this.key = key;
        this.timestamp = timestamp;
        this.type = type;
        this.priority = priority;
    }

    public String getKey() {
        return key;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Short getType() {
        return type;
    }

    public void setType(Short type) {
        this.type = type;
    }

    public Short getPriority() {
        return priority;
    }

    public void setPriority(Short priority) {
        this.priority = priority;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((key == null) ? 0 : key.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Record other = (Record) obj;
        if (key == null) {
            if (other.key != null)
                return false;
        } else if (!key.equals(other.key))
            return false;
        return true;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(key);
        builder.append(" ");
        builder.append(timestamp);
        builder.append(" ");
        builder.append(type);
        builder.append(" ");
        builder.append(priority);
        return builder.toString();
    }

    @Override
    public int compareTo(Record rec) {
        return this.key.compareTo(rec.key);
    }
}
