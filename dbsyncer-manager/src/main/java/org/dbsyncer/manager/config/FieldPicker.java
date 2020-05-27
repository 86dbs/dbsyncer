package org.dbsyncer.manager.config;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.config.Field;
import org.dbsyncer.connector.config.Filter;
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

    public FieldPicker(TableGroup tableGroup, List<Filter> filter) {
        this.tableGroup = tableGroup;
    }

    public FieldPicker(TableGroup tableGroup, List<Filter> filter, List<Field> column, List<FieldMapping> fieldMapping) {
        this.tableGroup = tableGroup;
        init(filter, column, fieldMapping);
    }

    public Map<String, Object> getColumns(List<Object> list) {
        if (!CollectionUtils.isEmpty(list)) {
            Map<String, Object> data = new HashMap<>(indexSize);
            int size = list.size();
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
        // TODO 过滤
        //Map<String, Object> row = data.getData();
        return false;
    }

    private void init(List<Filter> filter, List<Field> column, List<FieldMapping> fieldMapping) {
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
        int i;

        public Node(String name, int i) {
            this.name = name;
            this.i = i;
        }
    }

}