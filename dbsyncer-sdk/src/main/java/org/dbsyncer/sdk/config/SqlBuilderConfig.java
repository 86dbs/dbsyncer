package org.dbsyncer.sdk.config;

import org.dbsyncer.sdk.connector.database.Database;
import org.dbsyncer.sdk.model.Field;

import java.util.List;

public class SqlBuilderConfig {

    private Database database;

    // 架构名
    private String schema;

    // 表名
    private String tableName;

    // 主键列表
    private List<String> primaryKeys;

    // 字段
    private List<Field> fields;

    // 过滤条件
    private String queryFilter;

    public SqlBuilderConfig(Database database, String schema, String tableName, List<String> primaryKeys,
                            List<Field> fields, String queryFilter) {
        this.database = database;
        this.schema = schema;
        this.tableName = tableName;
        this.primaryKeys = primaryKeys;
        this.fields = fields;
        this.queryFilter = queryFilter;
    }

    public Database getDatabase() {
        return database;
    }

    public String getSchema() {
        return schema;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public List<Field> getFields() {
        return fields;
    }

    public String getQueryFilter() {
        return queryFilter;
    }
}