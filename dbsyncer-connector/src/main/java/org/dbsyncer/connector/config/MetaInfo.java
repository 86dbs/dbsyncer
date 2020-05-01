package org.dbsyncer.connector.config;

import java.util.List;

/**
 * 连接器基本信息
 *
 * @author AE86
 * @ClassName: MetaInfo
 * @Description: 包括连接器的配置、元信息、总条数
 * @date: 2017年7月20日 下午3:37:59
 */
public class MetaInfo {

    /**
     * 属性字段
     * 格式：[{"name":"ID","typeName":"INT","type":"4"},{"name":"NAME","typeName":"VARCHAR","type":"12"}]
     */
    private List<Field> column;

    /**
     * 总条数
     */
    private long count;

    public MetaInfo() {
    }

    public MetaInfo(List<Field> column, long count) {
        super();
        this.column = column;
        this.count = count;
    }

    public List<Field> getColumn() {
        return column;
    }

    public long getCount() {
        return count;
    }

    @Override
    public String toString() {
        return new StringBuilder().append("MetaInfo{").append("column=").append(column).append(", count=").append(count).append('}').toString();
    }
}
