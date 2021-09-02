package org.dbsyncer.plugin.enums;

import org.dbsyncer.common.util.StringUtil;

/**
 * 文件格式
 *
 * @author AE86
 * @version 1.0.0
 * @date 2021/2/23 23:21
 */
public enum FileSuffixEnum {

    /**
     * jar
     */
    JAR("jar");

    private String name;

    FileSuffixEnum(String name) {
        this.name = name;
    }

    /**
     * 获取文件类型
     *
     * @param suffix
     * @return
     */
    public static FileSuffixEnum getFileSuffix(String suffix) {
        for (FileSuffixEnum e : FileSuffixEnum.values()) {
            if (StringUtil.equals(suffix, e.getName())) {
                return e;
            }
        }
        return null;
    }

    public String getName() {
        return name;
    }
}