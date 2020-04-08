package org.dbsyncer.parser.convert;

import org.dbsyncer.parser.enums.ConvertEnum;

import java.util.List;

/**
 * 字段转换
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/1 14:04
 */
public class Convert {

    /**
     * 字段名称
     */
    private String name;

    /**
     * 转换名称
     * @see ConvertEnum
     */
    private String convertName;

    /**
     * 转换方式
     *
     * @see ConvertEnum
     */
    private String convertCode;

    /**
     * 转换参数
     *
     * @see ConvertEnum
     */
    private String args;

    public String getName() {
        return name;
    }

    public String getConvertName() {
        return convertName;
    }

    public String getConvertCode() {
        return convertCode;
    }

    public String getArgs() {
        return args;
    }
}
