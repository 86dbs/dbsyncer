package org.dbsyncer.connector.util;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.connector.model.Table;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
    public static List<String> findTablePrimaryKeys(Table table) {
        if (null == table) {
            throw new ConnectorException("The table is null.");
        }

        // 获取表同步的主键字段
        List<String> primaryKeys = new ArrayList<>();
        if (!CollectionUtils.isEmpty(table.getColumn())) {
            table.getColumn().forEach(c -> {
                if (c.isPk()) {
                    primaryKeys.add(c.getName());
                }
            });
        }

        // 如果存在表字段映射关系，没有配置主键则抛出异常提示
        if (!CollectionUtils.isEmpty(table.getColumn()) && CollectionUtils.isEmpty(primaryKeys)) {
            throw new ConnectorException(String.format("目标表 %s 缺少主键.", table.getName()));
        }
        return Collections.unmodifiableList(primaryKeys);
    }

    /**
     * 返回主键字段类型
     *
     * @param fields
     * @return
     */
    public static Map<String, Integer> findPrimaryKeyType(List<Field> fields) {
        Map<String, Integer> map = new HashMap<>();
        if (!CollectionUtils.isEmpty(fields)) {
            fields.forEach(field -> {
                if (field.isPk()) {
                    map.put(field.getName(), field.getType());
                }
            });
        }

        return Collections.unmodifiableMap(map);
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
     * 游标主键要包含数字类型，否则会导致分页失效
     *
     * @param sql
     * @param primaryKeys
     * @param quotation
     * @param join
     * @param typeAliases
     * @param skipFirst
     */
    public static void buildCursorSql(StringBuilder sql, List<String> primaryKeys, String quotation, String join, Map<String, Integer> typeAliases, boolean skipFirst) {
        AtomicBoolean added = new AtomicBoolean();
        AtomicBoolean supportedCursor = new AtomicBoolean();
        primaryKeys.forEach(pk -> {
            if (!typeAliases.containsKey(pk)) {
                throw new ConnectorException(String.format("Can't find type of primary key %s", pk));
            }

            // skip first pk
            if (!skipFirst || added.get()) {
                if (StringUtil.isNotBlank(join)) {
                    sql.append(join);
                }
            }
            sql.append(quotation).append(pk).append(quotation);

            Integer pkType = typeAliases.get(pk);
            if (pkType == Types.NUMERIC || pkType == Types.BIGINT || pkType == Types.INTEGER || pkType == Types.TINYINT || pkType == Types.SMALLINT) {
                sql.append(" > ? ");
                supportedCursor.set(true);
            } else {
                sql.append(" = ? ");
            }
            added.set(true);
        });

        if (!supportedCursor.get()) {
            throw new ConnectorException("不支持游标查询，主键至少要有一个为数字类型");
        }
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