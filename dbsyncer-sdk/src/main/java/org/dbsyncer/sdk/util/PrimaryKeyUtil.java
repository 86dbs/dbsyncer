/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.util;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Table;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public abstract class PrimaryKeyUtil {

    /**
     * 返回主键名称
     *
     * @param table
     * @return
     */
    public static List<String> findTablePrimaryKeys(Table table) {
        if (null == table) {
            throw new SdkException("The table is null.");
        }

        // 获取表同步的主键字段
        List<String> primaryKeys = findPrimaryKeyFields(table.getColumn()).stream().map(Field::getName).collect(Collectors.toList());

        // 如果存在表字段映射关系，没有配置主键则抛出异常提示
        if (!CollectionUtils.isEmpty(table.getColumn()) && CollectionUtils.isEmpty(primaryKeys)) {
            throw new SdkException(String.format("表 %s 缺少主键.", table.getName()));
        }
        return primaryKeys;
    }

    /**
     * 返回主键属性字段集合
     *
     * @param fields
     * @return
     */
    public static List<Field> findExistPrimaryKeyFields(List<Field> fields) {
        List<Field> list = findPrimaryKeyFields(fields);
        if (CollectionUtils.isEmpty(list)) {
            throw new SdkException("主键为空");
        }
        return list;
    }

    /**
     * 返回主键属性字段集合
     *
     * @param fields
     * @return
     */
    public static List<Field> findPrimaryKeyFields(List<Field> fields) {
        List<Field> list = new ArrayList<>();
        if (!CollectionUtils.isEmpty(fields)) {
            Set<String> mark = new HashSet<>();
            fields.forEach(f -> {
                if (f.isPk() && !mark.contains(f.getName())) {
                    list.add(f);
                    mark.add(f.getName());
                }
            });
        }
        return Collections.unmodifiableList(list);
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
     * 游标主键必须为可比较类型（数字、字符、日期时间等），否则会导致分页失效
     * 
     * <p>支持的类型：
     * <ul>
     *   <li>数字类型：NUMERIC, BIGINT, INTEGER, TINYINT, SMALLINT</li>
     *   <li>字符类型：VARCHAR, CHAR, NCHAR, NVARCHAR, LONGVARCHAR（字典序比较）</li>
     *   <li>日期时间类型：DATE, TIME, TIMESTAMP（按时间顺序比较）</li>
     * </ul>
     * </p>
     *
     * @param fields
     * @return
     */
    public static boolean isSupportedCursor(List<Field> fields) {
        if (CollectionUtils.isEmpty(fields)) {
            return false;
        }

        Map<String, Integer> typeAliases = new HashMap<>();
        fields.forEach(field -> {
            if (field.isPk()) {
                typeAliases.put(field.getName(), field.getType());
            }
        });

        for (Integer pkType : typeAliases.values()) {
            if (!isSupportedCursorType(pkType)) {
                return false;
            }
        }
        return !CollectionUtils.isEmpty(typeAliases);
    }

    /**
     * 是否支持游标类型
     * 
     * <p>支持可比较类型，包括：
     * <ul>
     *   <li>数字类型：可直接使用 > 比较</li>
     *   <li>字符类型：使用字典序比较</li>
     *   <li>日期时间类型：按时间顺序比较</li>
     * </ul>
     * </p>
     * <p>注意：不支持 CLOB/NCLOB/BLOB 等大对象类型作为游标。</p>
     *
     * @param type JDBC类型
     * @return 是否支持
     */
    private static boolean isSupportedCursorType(Integer type) {
        // 数字类型（支持直接比较）
        if (type == Types.NUMERIC || type == Types.BIGINT || type == Types.INTEGER 
                || type == Types.TINYINT || type == Types.SMALLINT
                || type == Types.DECIMAL || type == Types.FLOAT || type == Types.DOUBLE 
                || type == Types.REAL) {
            return true;
        }
        // 字符类型（支持字典序比较）
        if (type == Types.VARCHAR || type == Types.CHAR || type == Types.NCHAR 
                || type == Types.NVARCHAR || type == Types.LONGVARCHAR) {
            return true;
        }
        // 日期时间类型（支持时间顺序比较）
        if (type == Types.DATE || type == Types.TIME || type == Types.TIMESTAMP) {
            return true;
        }
        return false;
    }

    /**
     * 获取最新游标值
     *
     * @param data 数据列表
     * @param primaryKeys 主键字段名列表
     * @return 游标值数组，如果数据为空或主键列表为空则返回null
     */
    public static Object[] getLastCursors(List<Map> data, List<String> primaryKeys) {
        if (CollectionUtils.isEmpty(data) || CollectionUtils.isEmpty(primaryKeys)) {
            return null;
        }
        Map last = data.get(data.size() - 1);
        if (last == null || last.isEmpty()) {
            return null;
        }
        
        Object[] cursors = new Object[primaryKeys.size()];
        int i = 0;
        for (String pk : primaryKeys) {
            cursors[i++] = last.get(pk);
        }
        return cursors;
    }

    public static Object[] getLastCursors(String str) {
        Object[] cursors = null;
        if (StringUtil.isNotBlank(str)) {
            String[] split = StringUtil.split(str, StringUtil.COMMA);
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