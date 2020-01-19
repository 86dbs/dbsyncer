package org.dbsyncer.parser.model;

import org.dbsyncer.connector.config.Filter;
import org.dbsyncer.parser.convert.FieldConvert;
import org.dbsyncer.plugin.config.Plugin;

import java.util.List;

public abstract class AbstractConfigModel extends ConfigModel {

    // 过滤条件
    private Filter filter;

    // 转换配置
    private List<FieldConvert> fieldConvert;

    // 插件配置
    private Plugin plugin;

    public Filter getFilter() {
        return filter;
    }

    public void setFilter(Filter filter) {
        this.filter = filter;
    }

    public List<FieldConvert> getFieldConvert() {
        return fieldConvert;
    }

    public void setFieldConvert(List<FieldConvert> fieldConvert) {
        this.fieldConvert = fieldConvert;
    }

    public Plugin getPlugin() {
        return plugin;
    }

    public void setPlugin(Plugin plugin) {
        this.plugin = plugin;
    }

}