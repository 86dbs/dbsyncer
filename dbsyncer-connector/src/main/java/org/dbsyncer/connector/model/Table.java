package org.dbsyncer.connector.model;

import org.dbsyncer.connector.enums.TableTypeEnum;

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
     * 主键
     */
    private String primaryKey;

    /**
     * 属性字段
     * 格式：[{"name":"ID","typeName":"INT","type":"4"},{"name":"NAME","typeName":"VARCHAR","type":"12"}]
     */
    private List<Field> column;

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

    public Table(String name, String type, String primaryKey, List<Field> column) {
        this.name = name;
        this.type = type;
        this.primaryKey = primaryKey;
        this.column = column;
    }

    public String getName() {
        return name;
    }

    public Table setName(String name) {
        this.name = name;
        return this;
    }

    public String getType() {
        return type;
    }

    public Table setType(String type) {
        this.type = type;
        return this;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public List<Field> getColumn() {
        return column;
    }

    public Table setColumn(List<Field> column) {
        this.column = column;
        return this;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}