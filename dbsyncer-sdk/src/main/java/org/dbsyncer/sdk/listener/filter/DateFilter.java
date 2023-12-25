package org.dbsyncer.sdk.listener.filter;

import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.sdk.listener.QuartzFilter;

import java.sql.Date;
import java.time.Instant;

public class DateFilter implements QuartzFilter<Date> {

    private boolean begin;

    public DateFilter(boolean begin) {
        this.begin = begin;
    }

    @Override
    public Date getObject() {
        return new Date(Instant.now().toEpochMilli());
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