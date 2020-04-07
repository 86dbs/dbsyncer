package org.dbsyncer.biz.util;

import org.apache.commons.lang.StringUtils;

public abstract class CheckerTypeUtil {

    /**
     * 获取检查器类型
     *
     * @param type
     * @return
     */
    public static String getCheckerType(String type) {
        return toLowerCaseFirstOne(type).concat("ConfigChecker");
    }

    /**
     * 首字母转小写
     *
     * @param s
     * @return
     */
    private static String toLowerCaseFirstOne(String s) {
        if (StringUtils.isBlank(s) || Character.isLowerCase(s.charAt(0))) {
            return s;
        }
        return new StringBuilder().append(Character.toLowerCase(s.charAt(0))).append(s.substring(1)).toString();
    }

}