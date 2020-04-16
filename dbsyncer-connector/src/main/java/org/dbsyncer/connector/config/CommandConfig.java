package org.dbsyncer.connector.config;

import java.util.List;

/**
 * 查询同步参数模板
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/16 23:59
 */
public class CommandConfig {

    private String type;

    private Table table;

    private List<Filter> filter;

    public CommandConfig(String type, Table table) {
        this.type = type;
        this.table = table;
    }

    public CommandConfig(String type, Table table, List<Filter> filter) {
        this.type = type;
        this.table = table;
        this.filter = filter;
    }

    public String getType() {
        return type;
    }

    public Table getTable() {
        return table;
    }

    public List<Filter> getFilter() {
        return filter;
    }

}