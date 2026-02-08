package org.dbsyncer.sdk.model;

import org.dbsyncer.sdk.enums.TableTypeEnum;

import java.util.List;
import java.util.Properties;

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
     * {@link TableTypeEnum}
     */
    private String type;

    /**
     * 属性字段
     * 格式：[{"name":"ID","typeName":"INT","type":"4"},{"name":"NAME","typeName":"VARCHAR","type":"12"}]
     */
    private List<Field> column;

    // 总数
    private long count;

    /**
     * 扩展配置
     */
    private Properties extInfo = new Properties();

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

    public Properties getExtInfo() {
        return extInfo;
    }

    public void setExtInfo(Properties extInfo) {
        this.extInfo = extInfo;
    }

    @Override
    public Table clone() {
        Table table = new Table();
        table.setName(name);
        table.setType(type);
        table.setColumn(column);
        table.setCount(count);
        table.setExtInfo(extInfo);
        return table;
    }
}
