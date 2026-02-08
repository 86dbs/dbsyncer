/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

import java.util.List;
import java.util.Properties;

/**
 * 表基本信息
 *
 * @author AE86
 * @ClassName: MetaInfo
 * @Description: 包括字段信息、总条数
 * @date: 2017年7月20日 下午3:37:59
 */
public class MetaInfo {

    /**
     * 表
     */
    private String table;

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
     * 扩展配置
     */
    private Properties extInfo = new Properties();

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getTableType() {
        return tableType;
    }

    public void setTableType(String tableType) {
        this.tableType = tableType;
    }

    public List<Field> getColumn() {
        return column;
    }

    public void setColumn(List<Field> column) {
        this.column = column;
    }

    public Properties getExtInfo() {
        return extInfo;
    }

    public void setExtInfo(Properties extInfo) {
        this.extInfo = extInfo;
    }
}
