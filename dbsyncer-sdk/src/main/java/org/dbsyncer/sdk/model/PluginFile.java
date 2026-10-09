/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

import org.dbsyncer.common.util.StringUtil;

import java.util.regex.Pattern;

/**
 * 插件 JAR 文件描述（文件名、大小、摘要）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-09
 */
public class PluginFile {

    /**
     * 插件文件名请求头。
     */
    public static final String FILE_NAME_HEADER = "X-Plugin-File-Name";

    /**
     * 接收后继续分发。值为 {@code 1} 时生效。
     */
    public static final String RELAY_HEADER = "X-Plugin-Relay";

    /**
     * 单个插件文件上限，与页面上传限制一致。
     */
    public static final long MAX_BYTES = 128L * 1024 * 1024;

    private static final Pattern SAFE_NAME = Pattern.compile("[A-Za-z0-9._-]+\\.jar");

    private String name;

    private long size;

    private String sha256;

    /**
     * 文件名仅允许字母、数字、点、下划线、短横线，且以 {@code .jar} 结尾。
     *
     * @param name 文件名
     * @return 合法返回 true
     */
    public static boolean isSafeName(String name) {
        if (StringUtil.isBlank(name) || name.indexOf("..") >= 0) {
            return false;
        }
        return SAFE_NAME.matcher(name).matches();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public String getSha256() {
        return sha256;
    }

    public void setSha256(String sha256) {
        this.sha256 = sha256;
    }
}
