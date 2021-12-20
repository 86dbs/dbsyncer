package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.parser.convert.AbstractHandler;

import java.util.Date;

/**
 * Date转中国标准时间
 *
 * @author AE86
 * @version 1.0.0
 * @date 2021/12/20 23:04
 */
public class DateToChineseStandardTimeHandler extends AbstractHandler {

    @Override
    public Object convert(String args, Object value) {
        if (value instanceof Date) {
            Date d = (Date) value;
            value = DateFormatUtil.dateToChineseStandardTimeString(d);
        }
        return value;
    }

}