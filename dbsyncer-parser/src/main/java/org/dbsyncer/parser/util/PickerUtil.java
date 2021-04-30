package org.dbsyncer.parser.util;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.config.Field;
import org.dbsyncer.connector.config.Filter;
import org.dbsyncer.parser.model.*;
import org.springframework.beans.BeanUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class PickerUtil {

    private PickerUtil() {
    }

    /**
     * 合并参数配置、过滤条件、转换配置、插件配置、目标源字段、数据源字段
     *
     * @param mapping
     * @param tableGroup
     */
    public static TableGroup mergeTableGroupConfig(Mapping mapping, TableGroup tableGroup) {
        TableGroup group = new TableGroup();
        List<FieldMapping> fm = new ArrayList<>();
        tableGroup.getFieldMapping().forEach(f -> {
            FieldMapping m = new FieldMapping();
            BeanUtils.copyProperties(f, m);
            fm.add(m);
        });
        group.setFieldMapping(fm);
        group.setSourceTable(tableGroup.getSourceTable());
        group.setTargetTable(tableGroup.getTargetTable());
        group.setCommand(tableGroup.getCommand());

        // 参数配置(默认使用全局)
        group.setParams(CollectionUtils.isEmpty(tableGroup.getParams()) ? mapping.getParams() : tableGroup.getParams());
        // 过滤条件(默认使用全局)
        group.setFilter(CollectionUtils.isEmpty(tableGroup.getFilter()) ? mapping.getFilter() : tableGroup.getFilter());
        // 转换配置(默认使用全局)
        group.setConvert(CollectionUtils.isEmpty(tableGroup.getConvert()) ? mapping.getConvert() : tableGroup.getConvert());
        // 插件配置(默认使用全局)
        group.setPlugin(null == tableGroup.getPlugin() ? mapping.getPlugin() : tableGroup.getPlugin());

        // 合并增量配置/过滤条件/转换配置字段
        appendFieldMapping(mapping, group);
        return group;
    }

    public static Map<String, Field> convert2Map(List<Field> col) {
        return col.stream().collect(Collectors.toMap(Field::getName, f -> f));
    }

    private static void appendFieldMapping(Mapping mapping, TableGroup group) {
        final List<FieldMapping> fieldMapping = group.getFieldMapping();

        // 检查增量字段是否在映射关系中
        String eventFieldName = mapping.getListener().getEventFieldName();
        if (StringUtils.isNotBlank(eventFieldName)) {
            Map<String, Field> fields = convert2Map(group.getSourceTable().getColumn());
            addFieldMapping(fieldMapping, eventFieldName, fields, true);
        }

        // 检查过滤条件是否在映射关系中
        List<Filter> filter = group.getFilter();
        if (!CollectionUtils.isEmpty(filter)) {
            Map<String, Field> fields = convert2Map(group.getSourceTable().getColumn());
            filter.forEach(f -> addFieldMapping(fieldMapping, f.getName(), fields, true));
        }

        // 检查转换配置是否在映射关系中
        List<Convert> convert = group.getConvert();
        if (!CollectionUtils.isEmpty(convert)) {
            Map<String, Field> fields = convert2Map(group.getTargetTable().getColumn());
            convert.forEach(c -> addFieldMapping(fieldMapping, c.getName(), fields, false));
        }
    }

    private static void addFieldMapping(List<FieldMapping> fieldMapping, String name, Map<String, Field> fields, boolean checkSource) {
        if (StringUtils.isNotBlank(name)) {
            boolean exist = false;
            for (FieldMapping m : fieldMapping) {
                Field f = checkSource ? m.getSource() : m.getTarget();
                if (null == f) {
                    continue;
                }
                if (StringUtils.equals(f.getName(), name)) {
                    exist = true;
                    break;
                }
            }
            if (!exist && null != fields.get(name)) {
                FieldMapping fm = checkSource ? new FieldMapping(fields.get(name), null) : new FieldMapping(null, fields.get(name));
                fieldMapping.add(fm);
            }
        }
    }

}