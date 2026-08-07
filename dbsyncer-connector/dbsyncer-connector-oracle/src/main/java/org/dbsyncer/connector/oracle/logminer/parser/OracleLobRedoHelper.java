/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.logminer.parser;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.model.Field;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * LogMiner redo 中 LOB 占位识别（大字段常以 EMPTY_BLOB/EMPTY_CLOB 出现在 SQL_REDO）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-07 17:10
 */
public final class OracleLobRedoHelper {

    private OracleLobRedoHelper() {
    }

    /**
     * 行中需按主键回查覆盖的下标：LOB 列且值为 EMPTY_BLOB()/EMPTY_CLOB() 占位（INSERT/UPDATE 大字段常见）。
     */
    public static List<Integer> findEmptyLobPlaceholderIndexes(List<Field> fields, List<Object> data) {
        List<Integer> indexes = new ArrayList<>();
        if (CollectionUtils.isEmpty(fields) || CollectionUtils.isEmpty(data) || fields.size() != data.size()) {
            return indexes;
        }
        for (int i = 0; i < fields.size(); i++) {
            if (!isLobField(fields.get(i))) {
                continue;
            }
            if (isEmptyLobPlaceholder(data.get(i))) {
                indexes.add(i);
            }
        }
        return indexes;
    }

    public static boolean isEmptyLobPlaceholder(Object value) {
        if (value == null || OracleUnchangedValue.isUnchanged(value)) {
            return false;
        }
        String text = String.valueOf(value).trim();
        if (StringUtil.isBlank(text)) {
            return false;
        }
        String upper = text.toUpperCase(Locale.ROOT);
        return "EMPTY_BLOB()".equals(upper) || "EMPTY_CLOB()".equals(upper);
    }

    public static boolean isLobField(Field field) {
        if (field == null) {
            return false;
        }
        int type = field.getType();
        if (type == Types.BLOB || type == Types.CLOB || type == Types.NCLOB
                || type == Types.LONGVARBINARY || type == Types.LONGVARCHAR) {
            return true;
        }
        String typeName = field.getTypeName();
        if (StringUtil.isBlank(typeName)) {
            return false;
        }
        String upper = typeName.trim().toUpperCase(Locale.ROOT);
        return upper.contains("BLOB")
                || upper.contains("CLOB")
                || "LONG".equals(upper)
                || "LONG RAW".equals(upper);
    }
}
