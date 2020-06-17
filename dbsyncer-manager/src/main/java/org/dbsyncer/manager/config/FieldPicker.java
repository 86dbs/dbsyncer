package org.dbsyncer.manager.config;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.CompareFilter;
import org.dbsyncer.connector.config.Field;
import org.dbsyncer.connector.config.Filter;
import org.dbsyncer.connector.enums.FilterEnum;
import org.dbsyncer.connector.enums.OperationEnum;
import org.dbsyncer.parser.model.DataEvent;
import org.dbsyncer.parser.model.FieldMapping;
import org.dbsyncer.parser.model.TableGroup;
import org.springframework.util.Assert;

import java.util.*;
import java.util.stream.Collectors;

public class FieldPicker {

    private TableGroup tableGroup;
    private List<Node> index;
    private int indexSize;
    private boolean filterSwitch;
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
            index.parallelStream().forEach(node -> {
                if (node.i <= size) {
                    data.put(node.name, list.get(node.i));
                }
            });
            return data;
        }
        return Collections.EMPTY_MAP;
    }

    public TableGroup getTableGroup() {
        return tableGroup;
    }

    /**
     * 根据过滤条件过滤
     *
     * @param data
     * @return
     */
    public boolean filter(DataEvent data) {
        if (!filterSwitch) {
            return true;
        }
        final Map<String, Object> row = data.getData();
        // where (id > 1 and id < 100) or (id = 100 or id =101)
        // 或 关系(成立任意条件)
        CompareFilter filter = null;
        Object value = null;
        for (Filter f : or) {
            value = row.get(f.getName());
            if (null == value) {
                continue;
            }
            filter = FilterEnum.getCompareFilter(f.getFilter());
            if (filter.compare(String.valueOf(value), f.getValue())) {
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
            filter = FilterEnum.getCompareFilter(f.getFilter());
            if (!filter.compare(String.valueOf(value), f.getValue())) {
                return false;
            }
            pass = true;
        }

        return pass;
    }

    private void init(List<Filter> filter, List<Field> column, List<FieldMapping> fieldMapping) {
        // 解析过滤条件
        if ((filterSwitch = !CollectionUtils.isEmpty(filter))) {
            add = filter.stream().filter(f -> StringUtils.equals(f.getOperation(), OperationEnum.AND.getName())).collect(
                    Collectors.toList());
            or = filter.stream().filter(f -> StringUtils.equals(f.getOperation(), OperationEnum.OR.getName())).collect(Collectors.toList());
        }

        // column  => [1, 86, 0, 中文, 2020-05-15T12:17:22.000+0800, 备注信息]
        Assert.notEmpty(column, "读取字段不能为空.");
        Assert.notEmpty(fieldMapping, "映射关系不能为空.");

        // 找到同步字段 => [{source.name}]
        Set<String> key = fieldMapping.stream().map(m -> m.getSource().getName()).collect(Collectors.toSet());

        // 记录字段索引 [{"ID":0},{"NAME":1}]
        index = new LinkedList<>();
        int size = column.size();
        String k = null;
        for (int i = 0; i < size; i++) {
            k = column.get(i).getName();
            if (key.contains(k)) {
                index.add(new Node(k, i));
            }
        }
        Assert.notEmpty(index, "同步映射关系不能为空.");
        this.indexSize = index.size();
    }

    final class Node {
        // 属性
        String name;
        // 索引
        int    i;

        public Node(String name, int i) {
            this.name = name;
            this.i = i;
        }
    }

}