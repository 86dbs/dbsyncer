package org.dbsyncer.sdk.listener.filter;

import org.dbsyncer.sdk.listener.QuartzFilter;

import java.sql.Timestamp;
import java.time.Instant;

public class TimestampFilter implements QuartzFilter<Timestamp> {

    private boolean begin;

    public TimestampFilter(boolean begin) {
        this.begin = begin;
    }

    @Override
    public Timestamp getObject() {
        return new Timestamp(Instant.now().toEpochMilli());
    }

    @Override
    public Timestamp getObject(String s) {
        return new Timestamp(Long.parseLong(s));
    }

    @Override
    public String toString(Timestamp value) {
        return String.valueOf(value.getTime());
    }

    @Override
    public boolean begin() {
        return begin;
    }
}
