package org.dbsyncer.parser.model;

import org.dbsyncer.parser.flush.BufferRequest;
import org.dbsyncer.sdk.listener.ChangedEvent;
import org.dbsyncer.sdk.listener.event.RowChangedEvent;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 16:57
 */
public class WriterRequest extends AbstractWriter implements BufferRequest {

    private final List<Object> row;
    private final List<String> columnNames;  // CDC 捕获的列名列表（按数据顺序）

    public WriterRequest(ChangedEvent event) {
        setTraceId(event.getTraceId());
        setTypeEnum(event.getType());
        setChangedOffset(event.getChangedOffset());
        setTableName(event.getSourceTableName());
        setEvent(event.getEvent());
        setSql(event.getSql());
        this.row = event.getChangedRow();
        // 从 RowChangedEvent 中提取列名信息
        this.columnNames = extractColumnNames(event);
    }

    /**
     * 从事件中提取列名信息
     */
    private List<String> extractColumnNames(ChangedEvent event) {
        if (event instanceof RowChangedEvent) {
            return ((RowChangedEvent) event).getColumnNames();
        }
        return null;
    }

    @Override
    public String getMetaId() {
        return getChangedOffset().getMetaId();
    }

    public List<Object> getRow() {
        return row;
    }

    /**
     * 获取 CDC 捕获的列名列表（按数据顺序）
     * 
     * @return 列名列表，如果为 null 表示使用 TableGroup 的字段信息
     */
    public List<String> getColumnNames() {
        return columnNames;
    }

}