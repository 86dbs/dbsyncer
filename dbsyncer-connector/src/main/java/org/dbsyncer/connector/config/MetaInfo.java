package org.dbsyncer.connector.config;

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
    @Override
    public String toString() {
        return new StringBuilder().append("MetaInfo{").append("tableType=").append(tableType).append(", ").append("column=").append(column).append('}').toString();
    }
}