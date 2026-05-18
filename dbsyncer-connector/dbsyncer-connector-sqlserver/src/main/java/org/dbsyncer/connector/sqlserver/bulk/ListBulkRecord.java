/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver.bulk;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import org.dbsyncer.sdk.model.Field;

import java.sql.Types;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 基于内存 List&lt;Map&gt; + List&lt;Field&gt; 的 Bulk Copy 数据源适配器。
 * <p>
 * 将 dbsyncer 目标端的行集合（{@code List<Map<String, Object>>}）适配成 mssql-jdbc 的
 * {@link ISQLServerBulkRecord}，以便通过 {@code SQLServerBulkCopy} 一次性高速写入 SQL Server，
 * 避免 mssql-jdbc 参数化 INSERT 的"假批量"逐行 round-trip 问题。
 * <p>
 * 线程安全性：内部维护游标，<strong>不可跨线程共享</strong>。
 *
 * @author dbsyncer
 */
public class ListBulkRecord implements ISQLServerBulkRecord {

    private final List<Field> fields;
    private final List<Map> rows;
    private final Set<Integer> columnOrdinals;
    private int cursor = -1;

    /**
     * 构造 Bulk Copy 适配器。
     *
     * @param fields 目标字段列表（决定列顺序、类型、精度、标度）
     * @param rows   待写入的行数据集合
     */
    public ListBulkRecord(List<Field> fields, List<Map> rows) {
        this.fields = fields;
        this.rows = rows;
        this.columnOrdinals = new HashSet<>(fields.size());
        for (int i = 1; i <= fields.size(); i++) {
            this.columnOrdinals.add(i);
        }
    }

    @Override
    public Set<Integer> getColumnOrdinals() {
        return columnOrdinals;
    }

    @Override
    public String getColumnName(int column) {
        return fields.get(column - 1).getName();
    }

    @Override
    public int getColumnType(int column) {
        return fields.get(column - 1).getType();
    }

    @Override
    public int getPrecision(int column) {
        Field f = fields.get(column - 1);
        int p = f.getColumnSize();
        if (p <= 0) {
            return p;
        }
        // 关键修正：mssql-jdbc 8.x 的 SQLServerBulkCopy 在 bcp 协议层校验列长度时，
        //   - 数据流按 UTF-16 LE 编码（NCHAR/NVARCHAR/LONGNVARCHAR），字节数 = 字符数 × 2
        //   - 但 ISQLServerBulkRecord.getPrecision() 返回值常被驱动当作 "字节数" 上限
        // 单位错位会导致正常数据触发 "对 colid X 无效的列长度" 错误（典型表现：CnColor/EnColor 等中文列）。
        // 这里按列类型把 precision 对齐到 "字节数"：N 类型 ×2，普通 char/varchar 维持字符数即可（ANSI 1B/字符）。
        switch (f.getType()) {
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
                // 防御性 cap：SQL Server 单列 nvarchar 最大 4000 字符（再大需要 nvarchar(max)）
                // 翻倍后若超过 8000 字节，沿用 8000 即可（驱动会按 max 处理）
                return Math.min(p * 2, 8000);
            default:
                return p;
        }
    }

    @Override
    public int getScale(int column) {
        return fields.get(column - 1).getRatio();
    }

    @Override
    public boolean isAutoIncrement(int column) {
        // bulk copy 显式写入所有列（包含 identity 列值）；自增标识交由 SQLServerBulkCopyOptions.setKeepIdentity 控制。
        return false;
    }

    @Override
    public Object[] getRowData() throws SQLServerException {
        Map<String, Object> row = rows.get(cursor);
        Object[] data = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            data[i] = row.get(fields.get(i).getName());
        }
        return data;
    }

    @Override
    public boolean next() throws SQLServerException {
        return ++cursor < rows.size();
    }

    @Override
    public DateTimeFormatter getColumnDateTimeFormatter(int column) {
        // 使用 JDBC 驱动默认格式化，由驱动根据 column type 自行选择
        return null;
    }

    // ---------- 以下为 mssql-jdbc 接口抽象方法，本实现不关心时区格式化，全部留空 ----------

    @Override
    public void setTimestampWithTimezoneFormat(String dateTimeFormat) {
        // no-op
    }

    @Override
    public void setTimestampWithTimezoneFormat(DateTimeFormatter dateTimeFormatter) {
        // no-op
    }

    @Override
    public void setTimeWithTimezoneFormat(String timeFormat) {
        // no-op
    }

    @Override
    public void setTimeWithTimezoneFormat(DateTimeFormatter dateTimeFormatter) {
        // no-op
    }

    @Override
    public void addColumnMetadata(int positionInFile, String name, int jdbcType, int precision, int scale) {
        // 列元数据已经通过构造函数传入的 fields 提供，无需经由该方法再注入
    }

    @Override
    public void addColumnMetadata(int positionInFile, String name, int jdbcType, int precision, int scale,
                                  DateTimeFormatter dateTimeFormatter) {
        // 同上，列元数据已经通过 fields 提供
    }
}
