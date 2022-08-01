package org.dbsyncer.listener.enums;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.listener.quartz.QuartzFilter;
import org.dbsyncer.listener.quartz.filter.DateFilter;
import org.dbsyncer.listener.quartz.filter.TimestampFilter;
import org.dbsyncer.listener.quartz.filter.YesDateFilter;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/05/30 14:19
 */
public enum QuartzFilterEnum {

    /**
     * 时间戳(开始)
     */
    TIME_STAMP_BEGIN(1, "$timestamp_begin$", "系统时间戳(开始)", new TimestampFilter(true)),
    /**
     * 时间戳(结束)
     */
    TIME_STAMP_END(2, "$timestamp_end$", "系统时间戳(结束)", new TimestampFilter(false)),
    /**
     * 日期(开始)
     */
    DATE_BEGIN(3, "$date_begin$", "系统日期(开始)", new DateFilter(true)),
    /**
     * 日期(结束)
     */
    DATE_END(4, "$date_end$", "系统日期(结束)", new DateFilter(false)),
    /**
     * 昨天日期(开始)
     */
    DATE_YES_BEGIN(5, "$date_yes_begin$", "系统昨天日期(开始00:00:00)", new YesDateFilter(true)),
    /**
     * 昨天日期(结束)
     */
    DATE_YES_END(6, "$date_yes_end$", "系统昨天日期(结束23:59:59)", new YesDateFilter(false));


    private Integer index;
    private String type;
    private String message;
    private QuartzFilter quartzFilter;

    QuartzFilterEnum(Integer index, String type, String message, QuartzFilter quartzFilter) {
        this.index = index;
        this.type = type;
        this.message = message;
        this.quartzFilter = quartzFilter;
    }

    /**
     * @param type
     * @return
     */
    public static QuartzFilterEnum getQuartzFilterEnum(String type) {
        for (QuartzFilterEnum e : QuartzFilterEnum.values()) {
            if (StringUtil.equals(type, e.getType())) {
                return e;
            }
        }
        return null;
    }

    public Integer getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    public String getMessage() {
        return message;
    }

    public QuartzFilter getQuartzFilter() {
        return quartzFilter;
    }

}