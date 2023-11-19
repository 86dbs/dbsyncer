package org.dbsyncer.sdk.model;

import org.dbsyncer.sdk.enums.TableTypeEnum;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/15 23:58
 */
public class Table {

    /**
     * 表名
     */
    private String name;

    /**
     * 表类型[TABLE、VIEW、MATERIALIZED VIEW]
     */
    private String type;

    /**
     * 属性字段
     * 格式：[{"name":"ID","typeName":"INT","type":"4"},{"name":"NAME","typeName":"VARCHAR","type":"12"}]
     */
    private List<Field> column;

    /**
     * sql
     */
    private String sql;

    // 总数
    private long count;

    public Table() {
    }

    public Table(String name) {
        this(name, TableTypeEnum.TABLE.getCode());
    }

    public Table(String name, String type) {
        this(name, type, null, null);
    }

    public Table(String name, String type, List<Field> column, String sql) {
        this.name = name;
        this.type = type;
        this.column = column;
        this.sql = sql;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<Field> getColumn() {
        return column;
    }

    public void setColumn(List<Field> column) {
        this.column = column;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}