package org.dbsyncer.plugin.config;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/1 13:59
 */
public class Plugin {

    // 插件名称
    private String name;

    // 插件实现
    private String className;

    // 版本号
    private String version;

    // Jar名称
    private String fileName;

    // 是否系统预置
    private boolean unmodifiable;

    public Plugin() {
    }

    public Plugin(String name, String className, String version, String fileName, boolean unmodifiable) {
        this.name = name;
        this.className = className;
        this.version = version;
        this.fileName = fileName;
        this.unmodifiable = unmodifiable;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getClassName() {
        return className;
    }

    public Plugin setClassName(String className) {
        this.className = className;
        return this;
    }

    public String getVersion() {
        return version;
    }

    public Plugin setVersion(String version) {
        this.version = version;
        return this;
    }

    public String getFileName() {
        return fileName;
    }

    public Plugin setFileName(String fileName) {
        this.fileName = fileName;
        return this;
    }

    public boolean isUnmodifiable() {
        return unmodifiable;
    }

    public void setUnmodifiable(boolean unmodifiable) {
        this.unmodifiable = unmodifiable;
    }
}