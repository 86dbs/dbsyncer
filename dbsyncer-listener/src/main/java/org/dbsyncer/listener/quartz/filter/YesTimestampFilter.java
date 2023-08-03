package org.dbsyncer.listener.quartz.filter;

import org.dbsyncer.listener.quartz.QuartzFilter;

import java.sql.Timestamp;
import java.time.LocalDateTime;

public class YesTimestampFilter implements QuartzFilter {

    private boolean begin;

    public YesTimestampFilter(boolean begin) {
        this.begin = begin;
    }

    @Override
    public Object getObject() {
        if (begin) {
            // 2022-08-02 00:00:00
            return Timestamp.valueOf(LocalDateTime.now().minusDays(1).withHour(0).withMinute(0).withSecond(0).withNano(0));
        }
        // 2022-08-02 23:59:59
        return Timestamp.valueOf(LocalDateTime.now().minusDays(1).withHour(23).withMinute(59).withSecond(59).withNano(999999999));
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