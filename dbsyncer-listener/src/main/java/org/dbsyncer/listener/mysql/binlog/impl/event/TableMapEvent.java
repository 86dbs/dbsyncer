package org.dbsyncer.listener.mysql.binlog.impl.event;

import org.dbsyncer.listener.mysql.binlog.BinlogEventV4Header;
import org.dbsyncer.listener.mysql.common.glossary.Metadata;
import org.dbsyncer.listener.mysql.common.glossary.UnsignedLong;
import org.dbsyncer.listener.mysql.common.glossary.column.BitColumn;
import org.dbsyncer.listener.mysql.common.glossary.column.StringColumn;
import org.dbsyncer.listener.mysql.common.util.MySQLConstants;
import org.dbsyncer.listener.mysql.common.util.ToStringBuilder;

import java.util.Arrays;

/**
 * Used for row-based binary logging. This event precedes each row operation event.
 * It maps a table definition to a number, where the table definition consists of
 * database and table names and column definitions. The purpose of this event is to
 * enable replication when a table has different definitions on the master and slave.
 * Row operation events that belong to the same transaction may be grouped into sequences,
 * in which case each such sequence of events begins with a sequence of TABLE_MAP_EVENT events:
 * one per table used by events in the sequence.
 *
 * @ClassName: TableMapEvent
 * @author: AE86
 * @date: 2018年10月17日 下午2:29:31
 */
public final class TableMapEvent extends AbstractBinlogEventV4 {
    public static final int EVENT_TYPE = MySQLConstants.TABLE_MAP_EVENT;

    private long tableId;
    private int reserved;
    private int databaseNameLength;
    private StringColumn databaseName;
    private int tableNameLength;
    private StringColumn tableName;
    private UnsignedLong columnCount;
    private byte[] columnTypes;
    private UnsignedLong columnMetadataCount;
    private Metadata columnMetadata;
    private BitColumn columnNullabilities;

    public TableMapEvent() {
    }

    public TableMapEvent(BinlogEventV4Header header) {
        this.header = header;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("header", header)
                .append("tableId", tableId)
                .append("reserved", reserved)
                .append("databaseNameLength", databaseNameLength)
                .append("databaseName", databaseName)
                .append("tableNameLength", tableNameLength)
                .append("tableName", tableName)
                .append("columnCount", columnCount)
                .append("columnTypes", Arrays.toString(columnTypes))
                .append("columnMetadataCount", columnMetadataCount)
                .append("columnMetadata", columnMetadata)
                .append("columnNullabilities", columnNullabilities).toString();
    }

    public TableMapEvent copy() {
        final TableMapEvent r = new TableMapEvent();
        r.setHeader(this.header);
        r.setTableId(this.tableId);
        r.setReserved(this.reserved);
        r.setDatabaseNameLength(this.databaseNameLength);
        r.setDatabaseName(this.databaseName);
        r.setTableNameLength(this.tableNameLength);
        r.setTableName(this.tableName);
        r.setColumnCount(this.columnCount);
        r.setColumnTypes(this.columnTypes);
        r.setColumnMetadataCount(this.columnMetadataCount);
        r.setColumnMetadata(this.columnMetadata);
        r.setColumnNullabilities(this.columnNullabilities);
        return r;
    }

    public long getTableId() {
        return tableId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public int getReserved() {
        return reserved;
    }

    public void setReserved(int reserved) {
        this.reserved = reserved;
    }

    public int getDatabaseNameLength() {
        return databaseNameLength;
    }

    public void setDatabaseNameLength(int databaseNameLength) {
        this.databaseNameLength = databaseNameLength;
    }

    public StringColumn getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(StringColumn databaseName) {
        this.databaseName = databaseName;
    }

    public int getTableNameLength() {
        return tableNameLength;
    }

    public void setTableNameLength(int tableNameLength) {
        this.tableNameLength = tableNameLength;
    }

    public StringColumn getTableName() {
        return tableName;
    }

    public void setTableName(StringColumn tableName) {
        this.tableName = tableName;
    }

    public UnsignedLong getColumnCount() {
        return columnCount;
    }

    public void setColumnCount(UnsignedLong columnCount) {
        this.columnCount = columnCount;
    }

    public byte[] getColumnTypes() {
        return columnTypes;
    }

    public void setColumnTypes(byte[] columnTypes) {
        this.columnTypes = columnTypes;
    }

    public UnsignedLong getColumnMetadataCount() {
        return columnMetadataCount;
    }

    public void setColumnMetadataCount(UnsignedLong columnMetadataCount) {
        this.columnMetadataCount = columnMetadataCount;
    }

    public Metadata getColumnMetadata() {
        return columnMetadata;
    }

    public void setColumnMetadata(Metadata columnMetadata) {
        this.columnMetadata = columnMetadata;
    }

    public BitColumn getColumnNullabilities() {
        return columnNullabilities;
    }

    public void setColumnNullabilities(BitColumn columnNullabilities) {
        this.columnNullabilities = columnNullabilities;
    }
}
