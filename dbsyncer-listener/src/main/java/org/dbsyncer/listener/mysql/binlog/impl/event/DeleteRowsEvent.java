package org.dbsyncer.listener.mysql.binlog.impl.event;

import org.dbsyncer.listener.mysql.binlog.BinlogEventV4Header;
import org.dbsyncer.listener.mysql.common.glossary.Row;
import org.dbsyncer.listener.mysql.common.glossary.UnsignedLong;
import org.dbsyncer.listener.mysql.common.glossary.column.BitColumn;
import org.dbsyncer.listener.mysql.common.util.MySQLConstants;
import org.dbsyncer.listener.mysql.common.util.ToStringBuilder;

import java.util.List;

/**
 * Used for row-based binary logging. This event logs deletions of rows in a single table.
 *
 * @ClassName: DeleteRowsEvent
 * @author: AE86
 * @date: 2018年10月17日 下午2:24:57
 */
public final class DeleteRowsEvent extends AbstractRowEvent {
    //
    public static final int EVENT_TYPE = MySQLConstants.DELETE_ROWS_EVENT;

    //
    private UnsignedLong columnCount;
    private BitColumn usedColumns;
    private List<Row> rows;

    /**
     *
     */
    public DeleteRowsEvent() {
    }

    public DeleteRowsEvent(BinlogEventV4Header header) {
        this.header = header;
    }

    /**
     *
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("header", header)
                .append("tableId", tableId)
                .append("reserved", reserved)
                .append("columnCount", columnCount)
                .append("usedColumns", usedColumns)
                .append("rows", rows).toString();
    }

    /**
     *
     */
    public UnsignedLong getColumnCount() {
        return columnCount;
    }

    public void setColumnCount(UnsignedLong columnCount) {
        this.columnCount = columnCount;
    }

    public BitColumn getUsedColumns() {
        return usedColumns;
    }

    public void setUsedColumns(BitColumn usedColumns) {
        this.usedColumns = usedColumns;
    }

    public List<Row> getRows() {
        return rows;
    }

    public void setRows(List<Row> rows) {
        this.rows = rows;
    }
}
