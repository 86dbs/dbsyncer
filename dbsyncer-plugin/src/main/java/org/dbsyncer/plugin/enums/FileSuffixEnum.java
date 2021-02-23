package org.dbsyncer.plugin.enums;

import org.apache.commons.lang.StringUtils;

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
    JAR("jar"),
    /**
     * zip
     */
    ZIP("zip");

    private String name;

    FileSuffixEnum(String name) {
        this.name = name;
    }

    /**
     * 是否jar格式
     *
     * @param suffix
     * @return
     */
    public static boolean isJar(String suffix) {
        return StringUtils.equals(JAR.name, suffix);
    }

    public String getName() {
        return name;
    }

}