/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.util;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.config.WriterBatchConfig;
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
        List<String> primaryKeys = findPrimaryKeyFields(table.getColumn()).stream().map(f -> f.getName()).collect(Collectors.toList());

        // 如果存在表字段映射关系，没有配置主键则抛出异常提示
        if (!CollectionUtils.isEmpty(table.getColumn()) && CollectionUtils.isEmpty(primaryKeys)) {
            throw new SdkException(String.format("表 %s 缺少主键.", table.getName()));
        }
        return primaryKeys;
    }

    /**
     * 返回主键属性字段集合
     *
     * @param config
     * @return
     */
    public static List<Field> findConfigPrimaryKeyFields(WriterBatchConfig config) {
        if (null == config) {
            throw new SdkException("The config is null.");
        }

        List<Field> list = findPrimaryKeyFields(config.getFields());
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
     * 游标主键必须为数字类型，否则会导致分页失效
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
     * 是否支持游标类型(数字)
     *
     * @param type
     * @return
     */
    private static boolean isSupportedCursorType(Integer type) {
        return type == Types.NUMERIC || type == Types.BIGINT || type == Types.INTEGER || type == Types.TINYINT || type == Types.SMALLINT;
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