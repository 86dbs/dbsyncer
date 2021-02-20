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

    public Plugin() {
    }

    public Plugin(String name, String className) {
        this.name = name;
        this.className = className;
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
}