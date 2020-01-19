package org.dbsyncer.listener.mysql.binlog.impl.event;

import org.dbsyncer.listener.mysql.binlog.BinlogEventV4Header;
import org.dbsyncer.listener.mysql.common.glossary.Row;
import org.dbsyncer.listener.mysql.common.glossary.UnsignedLong;
import org.dbsyncer.listener.mysql.common.glossary.column.BitColumn;
import org.dbsyncer.listener.mysql.common.util.MySQLConstants;
import org.dbsyncer.listener.mysql.common.util.ToStringBuilder;

import java.util.List;

/**
 * Used for row-based binary logging. This event logs inserts of rows in a single table.
 *
 * @ClassName: WriteRowsEventV2
 * @author: AE86
 * @date: 2018年10月17日 下午2:31:28
 */
public final class WriteRowsEventV2 extends AbstractRowEvent {
    public static final int EVENT_TYPE = MySQLConstants.WRITE_ROWS_EVENT_V2;

    private int extraInfoLength;
    private byte extraInfo[];
    private UnsignedLong columnCount;
    private BitColumn usedColumns;
    private List<Row> rows;

    public WriteRowsEventV2() {
    }

    public WriteRowsEventV2(BinlogEventV4Header header) {
        this.header = header;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("header", header)
                .append("tableId", tableId)
                .append("reserved", reserved)
                .append("extraInfoLength", extraInfoLength)
                .append("extraInfo", extraInfo)
                .append("columnCount", columnCount)
                .append("usedColumns", usedColumns)
                .append("rows", rows).toString();
    }

    public int getExtraInfoLength() {
        return extraInfoLength;
    }

    public void setExtraInfoLength(int extraInfoLength) {
        this.extraInfoLength = extraInfoLength;
    }

    public byte[] getExtraInfo() {
        return extraInfo;
    }

    public void setExtraInfo(byte[] extraInfo) {
        this.extraInfo = extraInfo;
    }

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
