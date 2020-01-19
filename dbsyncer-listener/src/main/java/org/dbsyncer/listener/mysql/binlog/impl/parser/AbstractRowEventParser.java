package org.dbsyncer.listener.mysql.binlog.impl.parser;

import org.dbsyncer.listener.mysql.binlog.BinlogRowEventFilter;
import org.dbsyncer.listener.mysql.binlog.impl.event.TableMapEvent;
import org.dbsyncer.listener.mysql.binlog.impl.filter.BinlogRowEventFilterImpl;
import org.dbsyncer.listener.mysql.common.glossary.Column;
import org.dbsyncer.listener.mysql.common.glossary.Metadata;
import org.dbsyncer.listener.mysql.common.glossary.Row;
import org.dbsyncer.listener.mysql.common.glossary.column.*;
import org.dbsyncer.listener.mysql.common.util.CodecUtils;
import org.dbsyncer.listener.mysql.common.util.MySQLConstants;
import org.dbsyncer.listener.mysql.common.util.MySQLUtils;
import org.dbsyncer.listener.mysql.io.XInputStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractRowEventParser extends AbstractBinlogEventParser {
    protected BinlogRowEventFilter rowEventFilter;

    public AbstractRowEventParser(int eventType) {
        super(eventType);
        this.rowEventFilter = new BinlogRowEventFilterImpl();
    }

    public BinlogRowEventFilter getRowEventFilter() {
        return rowEventFilter;
    }

    public AbstractRowEventParser setRowEventFilter(BinlogRowEventFilter filter) {
        this.rowEventFilter = filter;
        return this;
    }

    protected Row parseRow(XInputStream is, TableMapEvent tme, BitColumn usedColumns)
            throws IOException {
        int unusedColumnCount = 0;
        final byte[] types = tme.getColumnTypes();
        final Metadata metadata = tme.getColumnMetadata();
        final int nColumnsUsed = usedColumns.getSetBitCount();
        final BitColumn nullColumns = is.readBit(nColumnsUsed);
        final List<Column> columns = new ArrayList<Column>(nColumnsUsed);
        for (int i = 0; i < types.length; ++i) {
            int length = 0;
            final int meta = metadata.getMetadata(i);
            int type = CodecUtils.toUnsigned(types[i]);
            if (type == MySQLConstants.TYPE_STRING && meta > 256) {
                final int meta0 = meta >> 8;
                final int meta1 = meta & 0xFF;
                if ((meta0 & 0x30) != 0x30) { // a long CHAR() field: see #37426
                    type = meta0 | 0x30;
                    length = meta1 | (((meta0 & 0x30) ^ 0x30) << 4);
                } else {
                    switch (meta0) {
                        case MySQLConstants.TYPE_SET:
                        case MySQLConstants.TYPE_ENUM:
                        case MySQLConstants.TYPE_STRING:
                            type = meta0;
                            length = meta1;
                            break;
                        default:
                            throw new RuntimeException("assertion failed, unknown column type: " + type);
                    }
                }
            }

            if (!usedColumns.get(i)) {
                unusedColumnCount++;
                continue;
            } else if (nullColumns.get(i - unusedColumnCount)) {
                columns.add(NullColumn.valueOf(type));
                continue;
            }

            switch (type) {
                case MySQLConstants.TYPE_TINY:
                    columns.add(TinyColumn.valueOf(is.readSignedInt(1)));
                    break;
                case MySQLConstants.TYPE_SHORT:
                    columns.add(ShortColumn.valueOf(is.readSignedInt(2)));
                    break;
                case MySQLConstants.TYPE_INT24:
                    columns.add(Int24Column.valueOf(is.readSignedInt(3)));
                    break;
                case MySQLConstants.TYPE_LONG:
                    columns.add(LongColumn.valueOf(is.readSignedInt(4)));
                    break;
                case MySQLConstants.TYPE_LONGLONG:
                    columns.add(LongLongColumn.valueOf(is.readSignedLong(8)));
                    break;
                case MySQLConstants.TYPE_FLOAT:
                    columns.add(FloatColumn.valueOf(Float.intBitsToFloat(is.readInt(4))));
                    break;
                case MySQLConstants.TYPE_DOUBLE:
                    columns.add(DoubleColumn.valueOf(Double.longBitsToDouble(is.readLong(8))));
                    break;
                case MySQLConstants.TYPE_YEAR:
                    columns.add(YearColumn.valueOf(MySQLUtils.toYear(is.readInt(1))));
                    break;
                case MySQLConstants.TYPE_DATE:
                    columns.add(DateColumn.valueOf(MySQLUtils.toDate(is.readInt(3))));
                    break;
                case MySQLConstants.TYPE_TIME:
                    columns.add(TimeColumn.valueOf(MySQLUtils.toTime(is.readInt(3))));
                    break;
                case MySQLConstants.TYPE_DATETIME:
                    columns.add(DatetimeColumn.valueOf(is.readLong(8)));
                    break;
                case MySQLConstants.TYPE_TIMESTAMP:
                    columns.add(TimestampColumn.valueOf(MySQLUtils.toTimestamp(is.readLong(4))));
                    break;
                case MySQLConstants.TYPE_ENUM:
                    columns.add(EnumColumn.valueOf(is.readInt(length)));
                    break;
                case MySQLConstants.TYPE_SET:
                    columns.add(SetColumn.valueOf(is.readLong(length)));
                    break;
                case MySQLConstants.TYPE_BIT:
                    final int bitLength = (meta >> 8) * 8 + (meta & 0xFF);
                    columns.add(is.readBit(bitLength, false));
                    break;
                case MySQLConstants.TYPE_BLOB:
                    final int blobLength = is.readInt(meta);
                    columns.add(is.readFixedLengthString(blobLength));
                    break;
                case MySQLConstants.TYPE_GEOMETRY:
                    // 暂时不考虑该类型实现
                    columns.add(null);
//				final int geomLength = is.readInt(meta);
//				final int _unknown = is.readInt(4);
//				final WKBReader reader = new WKBReader();
//
//				try {
//					final Geometry g = reader.read(is.readBytes(geomLength - 4));
//					columns.add(GeometryColumn.valueOf(g));
//				} catch ( ParseException e ) {
//					throw new RuntimeException("Could not parse geometry, unknown column was " + _unknown, e);
//				}
                    break;
                case MySQLConstants.TYPE_NEWDECIMAL:
                    final int precision = meta & 0xFF;
                    final int scale = meta >> 8;
                    final int decimalLength = MySQLUtils.getDecimalBinarySize(precision, scale);
                    columns.add(DecimalColumn.valueOf(MySQLUtils.toDecimal(precision, scale, is.readBytes(decimalLength)), precision, scale));
                    break;
                case MySQLConstants.TYPE_STRING:
                    final int stringLength = length < 256 ? is.readInt(1) : is.readInt(2);
                    columns.add(is.readFixedLengthString(stringLength));
                    break;
                case MySQLConstants.TYPE_VARCHAR:
                case MySQLConstants.TYPE_VAR_STRING:
                    final int varcharLength = meta < 256 ? is.readInt(1) : is.readInt(2);
                    columns.add(is.readFixedLengthString(varcharLength));
                    break;
                case MySQLConstants.TYPE_TIME2:
                    final int value1 = is.readInt(3, false);
                    final int metaLength1 = (meta + 1) / 2;
                    final int nanos1 = is.readInt(metaLength1, false);
                    java.sql.Timestamp ts = MySQLUtils.time2toTimestamp(value1, nanos1, metaLength1);
                    columns.add(Time2Column.valueOf(ts));
                    break;
                case MySQLConstants.TYPE_DATETIME2:
                    final long value2 = is.readLong(5, false);
                    final int metaLength2 = (meta + 1) / 2;
                    final int nanos2 = is.readInt(metaLength2, false);
                    java.sql.Timestamp ts2 = MySQLUtils.datetime2ToTimestamp(value2, nanos2, metaLength2);
                    columns.add(Datetime2Column.valueOf(ts2));
                    break;
                case MySQLConstants.TYPE_TIMESTAMP2:
                    final long value3 = is.readLong(4, false);
                    final int metaLength3 = (meta + 1) / 2;
                    final int nanos3 = is.readInt(metaLength3, false);
                    columns.add(Timestamp2Column.valueOf(MySQLUtils.timestamp2ToTimestamp(value3, nanos3, metaLength3)));
                    break;
                default:
                    throw new RuntimeException("assertion failed, unknown column type: " + type);
            }
        }
        return new Row(columns);
    }
}
