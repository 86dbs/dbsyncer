package org.dbsyncer.listener.quartz.filter;

import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.listener.quartz.QuartzFilter;

import java.sql.Date;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class YesDateFilter implements QuartzFilter {

    private boolean begin;

    public YesDateFilter(boolean begin) {
        this.begin = begin;
    }

    @Override
    public Object getObject() {
        LocalDateTime localDateTime;
        if (!begin) {
            // 2022-08-02 23:59:59
            localDateTime = LocalDateTime.now().withHour(23).withMinute(59).withSecond(59).withNano(999999999);
        } else {
            // 2022-08-02 00:00:00
            localDateTime = LocalDateTime.now().withHour(0).withMinute(0).withSecond(0).withNano(0);
        }
        return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
    }

    @Override
    public Object getObject(String s) {
        return DateFormatUtil.stringToDate(s);
    }

    @Override
    public String toString(Object value) {
        return DateFormatUtil.dateToString((Date) value);
    }

    @Override
    public boolean begin() {
        return begin;
    }

}
