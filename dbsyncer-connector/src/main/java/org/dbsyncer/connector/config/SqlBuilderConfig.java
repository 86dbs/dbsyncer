package org.dbsyncer.connector.config;

import org.dbsyncer.connector.database.Database;
import org.dbsyncer.connector.model.Field;

import java.util.List;

public class SqlBuilderConfig {

    private Database database;
    private CommandConfig commandConfig;
    // 架构名
    private String schema;
    // 表名
    private String tableName;
    // 主键
    private String pk;
    // 字段
    private List<Field> fields;
    // 过滤条件
    private String queryFilter;
    // 引号
    private String quotation;

    public SqlBuilderConfig(Database database, CommandConfig commandConfig, String schema, String tableName, String pk, List<Field> fields, String queryFilter, String quotation) {
        this.database = database;
        this.commandConfig = commandConfig;
        this.schema = schema;
        this.tableName = tableName;
        this.pk = pk;
        this.fields = fields;
        this.queryFilter = queryFilter;
        this.quotation = quotation;
    }

    public Database getDatabase() {
        return database;
    }

    public CommandConfig getCommandConfig() {
        return commandConfig;
    }

    public String getSchema() {
        return schema;
    }

    public String getTableName() {
        return tableName;
    }

    public String getPk() {
        return pk;
    }

    public List<Field> getFields() {
        return fields;
    }

    public String getQueryFilter() {
        return queryFilter;
    }

    public String getQuotation() {
        return quotation;
    }
}