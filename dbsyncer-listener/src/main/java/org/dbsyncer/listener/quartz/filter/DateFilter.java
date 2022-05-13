package org.dbsyncer.listener.quartz.filter;

import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.listener.quartz.QuartzFilter;

import java.sql.Date;
import java.time.LocalDate;

public class DateFilter implements QuartzFilter {

    private boolean begin;

    public DateFilter(boolean begin) {
        this.begin = begin;
    }

    @Override
    public Object getObject() {
        return Date.valueOf(LocalDate.now());
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
