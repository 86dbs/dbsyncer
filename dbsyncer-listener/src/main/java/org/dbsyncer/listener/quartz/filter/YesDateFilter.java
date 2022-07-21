package org.dbsyncer.listener.quartz.filter;

import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.listener.quartz.QuartzFilter;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class YesDateFilter implements QuartzFilter {

    private boolean begin;

    public YesDateFilter(boolean begin) {
        this.begin = begin;
    }

    @Override
    public Object getObject() {
        Date as = new Date(System.currentTimeMillis() - 24 * 60 * 60 * 1000);
        SimpleDateFormat matter1 = new SimpleDateFormat("yyyy-MM-dd");
        String time = matter1.format(as) + " 00:00:00";
        if (!begin) {
            time = matter1.format(as) + " 23:59:59";
        }
        matter1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            as = matter1.parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return as;
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
