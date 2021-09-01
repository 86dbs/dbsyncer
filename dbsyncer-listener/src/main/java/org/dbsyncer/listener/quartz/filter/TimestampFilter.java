package org.dbsyncer.listener.quartz.filter;

import org.dbsyncer.listener.quartz.QuartzFilter;

import java.sql.Timestamp;
import java.time.Instant;

public class TimestampFilter implements QuartzFilter {

    private boolean begin;

    public TimestampFilter(boolean begin) {
        this.begin = begin;
    }

    @Override
    public Object getObject() {
        return new Timestamp(Instant.now().toEpochMilli());
    }

    @Override
    public Object getObject(String s) {
        return new Timestamp(Long.parseLong(s));
    }

    @Override
    public String toString(Object value) {
        Timestamp ts = (Timestamp) value;
        return String.valueOf(ts.getTime());
    }

    @Override
    public boolean begin() {
        return begin;
    }
}
