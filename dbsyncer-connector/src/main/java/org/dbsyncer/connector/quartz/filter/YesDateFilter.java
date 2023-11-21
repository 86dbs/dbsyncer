package org.dbsyncer.connector.quartz.filter;

import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.connector.quartz.QuartzFilter;

import java.sql.Date;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class YesDateFilter implements QuartzFilter<Date> {

    private boolean begin;

    public YesDateFilter(boolean begin) {
        this.begin = begin;
    }

    @Override
    public Date getObject() {
        LocalDateTime localDateTime;
        if (!begin) {
            // 2022-08-02 23:59:59
            localDateTime = LocalDateTime.now().minusDays(1).withHour(23).withMinute(59).withSecond(59).withNano(999999999);
        } else {
            // 2022-08-02 00:00:00
            localDateTime = LocalDateTime.now().minusDays(1).withHour(0).withMinute(0).withSecond(0).withNano(0);
        }
        return new Date(localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
    }

    @Override
    public Date getObject(String s) {
        return DateFormatUtil.stringToDate(s);
    }

    @Override
    public String toString(Date value) {
        return DateFormatUtil.dateToString(value);
    }

    @Override
    public boolean begin() {
        return begin;
    }

}