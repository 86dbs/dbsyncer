package org.dbsyncer.parser.model;

import org.dbsyncer.connector.config.Filter;
import org.dbsyncer.plugin.config.Plugin;

import java.util.List;
import java.util.Map;

public abstract class AbstractConfigModel extends ConfigModel {

    // 全局参数
    private Map<String, String> params;

    // 过滤条件
    private List<Filter> filter;

    // 转换配置
    private List<Convert> convert;

    // 插件配置
    private Plugin plugin;

    public Map<String, String> getParams() {
        return params;
    }

    public AbstractConfigModel setParams(Map<String, String> params) {
        this.params = params;
        return this;
    }

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