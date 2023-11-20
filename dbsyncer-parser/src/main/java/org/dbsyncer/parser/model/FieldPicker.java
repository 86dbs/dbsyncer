/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.model;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.CompareFilter;
import org.dbsyncer.connector.enums.FilterEnum;
import org.dbsyncer.sdk.enums.OperationEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Filter;
import org.springframework.util.Assert;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FieldPicker {

    private TableGroup tableGroup;
    private List<Node> index;
    private int indexSize;
    private boolean enabledFilter;
    private List<Filter> add;
    private List<Filter> or;

    public FieldPicker(TableGroup tableGroup) {
        this.tableGroup = tableGroup;
    }

    public FieldPicker(TableGroup tableGroup, List<Filter> filter, List<Field> column, List<FieldMapping> fieldMapping) {
        this.tableGroup = tableGroup;
        init(filter, column, fieldMapping);
    }

    public Map<String, Object> getColumns(List<Object> list) {
        if (!CollectionUtils.isEmpty(list)) {
            Map<String, Object> data = new HashMap<>(indexSize);
            final int size = list.size() - 1;
            index.forEach(node -> {
                if (node.i <= size) {
                    data.put(node.name, list.get(node.i));
                }
            });
            return data;
        }
        return Collections.EMPTY_MAP;
    }

    /**
     * 根据过滤条件过滤
     *
     * @param row
     * @return
     */
    public boolean filter(Map<String, Object> row) {
        if (!enabledFilter) {
            return true;
        }
        // where (id > 1 and id < 100) or (id = 100 or id =101)
        // 或 关系(成立任意条件)
        Object value = null;
        for (Filter f : or) {
            value = row.get(f.getName());
            if (null == value) {
                continue;
            }
            if (compareValueWithFilter(f, value)) {
                return true;
            }
        }

        boolean pass = false;
        // 并 关系(成立所有条件)
        for (Filter f : add) {
            value = row.get(f.getName());
            if (null == value) {
                continue;
            }
            if (!compareValueWithFilter(f, value)) {
                return false;
            }
            pass = true;
        }

        return pass;
    }

    /**
     * 比较值是否满足过滤条件
     *
     * @param filter        过滤器
     * @param comparedValue 比较值
     * @return
     */
    private boolean compareValueWithFilter(Filter filter, Object comparedValue) {
        CompareFilter compareFilter = FilterEnum.getCompareFilter(filter.getFilter());
        if (null == filter) {
            return false;
        }

        // 支持时间比较
        if (comparedValue instanceof Timestamp) {
            Timestamp comparedTimestamp = (Timestamp) comparedValue;
            Timestamp filterTimestamp = DateFormatUtil.stringToTimestamp(filter.getValue());
            return compareFilter.compare(String.valueOf(comparedTimestamp.getTime()), String.valueOf(filterTimestamp.getTime()));
        }
        if (comparedValue instanceof Date) {
            Date comparedDate = (Date) comparedValue;
            Date filterDate = DateFormatUtil.stringToDate(filter.getValue());
            return compareFilter.compare(String.valueOf(comparedDate.getTime()), String.valueOf(filterDate.getTime()));
        }

        return compareFilter.compare(String.valueOf(comparedValue), filter.getValue());
    }

    private void init(List<Filter> filter, List<Field> column, List<FieldMapping> fieldMapping) {
        // column  => [1, 86, 0, 中文, 2020-05-15T12:17:22.000+0800, 备注信息]
        Assert.notEmpty(column, "读取字段不能为空.");
        Assert.notEmpty(fieldMapping, "映射关系不能为空.");

        // 解析过滤条件
        if ((enabledFilter = !CollectionUtils.isEmpty(filter))) {
            add = filter.stream().filter(f -> StringUtil.equals(f.getOperation(), OperationEnum.AND.getName())).collect(
                    Collectors.toList());
            or = filter.stream().filter(f -> StringUtil.equals(f.getOperation(), OperationEnum.OR.getName())).collect(Collectors.toList());
        }

        // 记录字段索引 [{"ID":0},{"NAME":1}]
        index = new LinkedList<>();
        int size = column.size();
        String k = null;
        Field field = null;
        for (int i = 0; i < size; i++) {
            field = column.get(i);
            k = field.isUnmodifiabled() ? field.getLabelName() : field.getName();
            index.add(new Node(k, i));
        }
        Assert.notEmpty(index, "同步映射关系不能为空.");
        this.indexSize = index.size();
    }

    public TableGroup getTableGroup() {
        return tableGroup;
    }

    final class Node {
        // 属性
        String name;
        // 索引
        int i;

        public Node(String name, int i) {
            this.name = name;
            this.i = i;
        }
    }

}