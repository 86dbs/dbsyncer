package org.dbsyncer.parser.model;

import org.dbsyncer.connector.config.Filter;
import org.dbsyncer.plugin.config.Plugin;

import java.util.List;

public abstract class AbstractConfigModel extends ConfigModel {

    // 过滤条件
    private List<Filter> filter;

    // 转换配置
    private List<Convert> convert;

    // 插件配置
    private Plugin plugin;

    public List<Filter> getFilter() {
        return filter;
    }

    public void setFilter(List<Filter> filter) {
        this.filter = filter;
    }

    public List<Convert> getConvert() {
        return convert;
    }

    public void setConvert(List<Convert> convert) {
        this.convert = convert;
    }

    public Plugin getPlugin() {
        return plugin;
    }

    public void setPlugin(Plugin plugin) {
        this.plugin = plugin;
    }

}