package org.dbsyncer.listener.mysql.binlog.impl.event;

import org.dbsyncer.listener.mysql.binlog.BinlogEventV4Header;
import org.dbsyncer.listener.mysql.common.glossary.Pair;
import org.dbsyncer.listener.mysql.common.glossary.Row;
import org.dbsyncer.listener.mysql.common.glossary.UnsignedLong;
import org.dbsyncer.listener.mysql.common.glossary.column.BitColumn;
import org.dbsyncer.listener.mysql.common.util.MySQLConstants;
import org.dbsyncer.listener.mysql.common.util.ToStringBuilder;

import java.util.List;

/**
 * Used for row-based binary logging. This event logs updates of rows in a single table.
 *
 * @ClassName: UpdateRowsEventV2
 * @author: AE86
 * @date: 2018年10月17日 下午2:30:27
 */
public final class UpdateRowsEventV2 extends AbstractRowEvent {
    public static final int EVENT_TYPE = MySQLConstants.UPDATE_ROWS_EVENT_V2;

    private int extraInfoLength;
    private byte extraInfo[];
    private UnsignedLong columnCount;
    private BitColumn usedColumnsBefore;
    private BitColumn usedColumnsAfter;
    private List<Pair<Row>> rows;

    public UpdateRowsEventV2() {
    }

    public UpdateRowsEventV2(BinlogEventV4Header header) {
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
                .append("usedColumnsBefore", usedColumnsBefore)
                .append("usedColumnsAfter", usedColumnsAfter)
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

    public BitColumn getUsedColumnsBefore() {
        return usedColumnsBefore;
    }

    public void setUsedColumnsBefore(BitColumn usedColumnsBefore) {
        this.usedColumnsBefore = usedColumnsBefore;
    }

    public BitColumn getUsedColumnsAfter() {
        return usedColumnsAfter;
    }

    public void setUsedColumnsAfter(BitColumn usedColumnsAfter) {
        this.usedColumnsAfter = usedColumnsAfter;
    }

    public List<Pair<Row>> getRows() {
        return rows;
    }

    public void setRows(List<Pair<Row>> rows) {
        this.rows = rows;
    }
}
