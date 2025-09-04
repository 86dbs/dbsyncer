package org.dbsyncer.parser.util;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.model.Convert;
import org.dbsyncer.parser.model.FieldMapping;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Filter;
import org.springframework.beans.BeanUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class PickerUtil {

    /**
     * 合并参数配置、过滤条件、转换配置、插件配置、目标源字段、数据源字段
     *
     * @param mapping
     * @param tableGroup
     */
    public static TableGroup mergeTableGroupConfig(Mapping mapping, TableGroup tableGroup) {
        TableGroup group = new TableGroup();
        BeanUtils.copyProperties(tableGroup, group);

        // 参数配置(默认使用全局)
        group.setParams(CollectionUtils.isEmpty(tableGroup.getParams()) ? mapping.getParams() : tableGroup.getParams());
        // 过滤条件(默认使用全局)
        group.setFilter(CollectionUtils.isEmpty(tableGroup.getFilter()) ? mapping.getFilter() : tableGroup.getFilter());
        // 转换配置(默认使用全局)
        group.setConvert(CollectionUtils.isEmpty(tableGroup.getConvert()) ? mapping.getConvert() : tableGroup.getConvert());
        // 插件配置(默认使用全局)
        group.setPlugin(null == tableGroup.getPlugin() ? mapping.getPlugin() : tableGroup.getPlugin());
        // 插件参数(默认使用全局)
        group.setPluginExtInfo(StringUtil.isBlank(tableGroup.getPluginExtInfo()) ? mapping.getPluginExtInfo() : tableGroup.getPluginExtInfo());

        // 合并增量配置/过滤条件/转换配置字段
        appendFieldMapping(mapping, group);
        return group;
    }

    public static Map<String, Field> convert2Map(List<Field> col) {
        if (CollectionUtils.isEmpty(col)) {
            return new HashMap<>();
        }
        return col.stream().collect(Collectors.toMap(Field::getName, f -> f, (k1, k2) -> k1));
    }

    private static void appendFieldMapping(Mapping mapping, TableGroup group) {
        final List<FieldMapping> fieldMapping = group.getFieldMapping();

        // 检查增量字段是否在映射关系中
        String eventFieldName = mapping.getListener().getEventFieldName();
        if (StringUtil.isNotBlank(eventFieldName)) {
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
        if (StringUtil.isNotBlank(name)) {
            boolean exist = false;
            for (FieldMapping m : fieldMapping) {
                Field f = checkSource ? m.getSource() : m.getTarget();
                if (null == f) {
                    continue;
                }
                if (StringUtil.equals(f.getName(), name)) {
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