package org.dbsyncer.sdk.model;

import java.util.List;

/**
 * 连接器基本信息
 *
 * @author AE86
 * @ClassName: MetaInfo
 * @Description: 包括字段信息、总条数
 * @date: 2017年7月20日 下午3:37:59
 */
public class MetaInfo {

    /**
     * 表类型
     */
    private String tableType;

    /**
     * 属性字段
     * 格式：[{"name":"ID","typeName":"INT","type":"4"},{"name":"NAME","typeName":"VARCHAR","type":"12"}]
     */
    private List<Field> column;

    /**
     * sql
     */
    private String sql;

    /**
     * 索引类型（ES）
     */
    private String indexType;

    public String getTableType() {
        return tableType;
    }

    public MetaInfo setTableType(String tableType) {
        this.tableType = tableType;
        return this;
    }

    public List<Field> getColumn() {
        return column;
    }

    public MetaInfo setColumn(List<Field> column) {
        this.column = column;
        return this;
    }

    public String getSql() {
        return sql;
    }

    public MetaInfo setSql(String sql) {
        this.sql = sql;
        return this;
    }

    public String getIndexType() {
        return indexType;
    }

    public MetaInfo setIndexType(String indexType) {
        this.indexType = indexType;
        return this;
    }

    @Override
    public String toString() {
        return new StringBuilder().append("MetaInfo{").append("tableType=").append(tableType).append(", ").append("column=").append(column).append('}').toString();
    }
}