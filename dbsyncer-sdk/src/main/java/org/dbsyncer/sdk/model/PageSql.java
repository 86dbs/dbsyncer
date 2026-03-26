package org.dbsyncer.sdk.model;

import java.util.List;

public class PageSql {

    private String querySql;

    // 过滤条件
    private String queryFilter;

    private List<String> primaryKeys;

    // 字段
    private List<Field> fields;

    public PageSql(String querySql, String queryFilter, List<String> primaryKeys, List<Field> fields) {
        this.querySql = querySql;
        this.queryFilter = queryFilter;
        this.primaryKeys = primaryKeys;
        this.fields = fields;
    }

    public String getQuerySql() {
        return querySql;
    }

    public String getQueryFilter() {
        return queryFilter;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public List<Field> getFields() {
        return fields;
    }
}
