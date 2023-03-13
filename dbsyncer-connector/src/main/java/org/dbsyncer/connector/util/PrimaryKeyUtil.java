package org.dbsyncer.connector.util;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.connector.model.Table;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class PrimaryKeyUtil {

    /**
     * 返回主键名称
     *
     * @param table
     * @return
     */
    public static List<String> findOriginalTablePrimaryKey(Table table) {
        if (null == table) {
            throw new ConnectorException("The table is null.");
        }

        // 获取自定义主键
        if (!CollectionUtils.isEmpty(table.getPrimaryKeys())) {
            return Collections.unmodifiableList(table.getPrimaryKeys());
        }

        // 获取表原始主键
        List<String> primaryKeys = new ArrayList<>();
        List<Field> column = table.getColumn();
        if (!CollectionUtils.isEmpty(column)) {
            for (Field c : column) {
                if (c.isPk() && !primaryKeys.contains(c.getName())) {
                    primaryKeys.add(c.getName());
                }
            }
        }

        if (CollectionUtils.isEmpty(primaryKeys)) {
            throw new ConnectorException(String.format("The primary key of table '%s' is null.", table.getName()));
        }
        return Collections.unmodifiableList(primaryKeys);
    }

    public static void buildSql(StringBuilder sql, List<String> primaryKeys, String quotation, String join, String value, boolean skipFirst) {
        AtomicBoolean added = new AtomicBoolean();
        primaryKeys.forEach(pk -> {
            // skip first pk
            if (!skipFirst || added.get()) {
                if (StringUtil.isNotBlank(join)) {
                    sql.append(join);
                }
            }
            sql.append(quotation).append(pk).append(quotation);
            if (StringUtil.isNotBlank(value)) {
                sql.append(value);
            }
            added.set(true);
        });
    }

    /**
     * 获取最新游标值
     *
     * @param data
     * @param primaryKeys
     * @return
     */
    public static Object[] getLastCursors(List<Map> data, List<String> primaryKeys) {
        if (CollectionUtils.isEmpty(data)) {
            return null;
        }
        Map last = data.get(data.size() - 1);
        Object[] cursors = new Object[primaryKeys.size()];
        Iterator<String> iterator = primaryKeys.iterator();
        int i = 0;
        while (iterator.hasNext()) {
            String pk = iterator.next();
            cursors[i] = last.get(pk);
            i++;
        }
        return cursors;
    }

    public static Object[] getLastCursors(String str) {
        Object[] cursors = null;
        if (StringUtil.isNotBlank(str)) {
            String[] split = StringUtil.split(str, ",");
            int length = split.length;
            cursors = new Object[length];
            for (int i = 0; i < length; i++) {
                String val = split[i];
                cursors[i] = NumberUtil.isCreatable(val) ? NumberUtil.toLong(val) : val;
            }
        }
        return cursors;
    }
}