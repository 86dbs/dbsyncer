package org.dbsyncer.parser.model;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.enums.FilterEnum;
import org.dbsyncer.sdk.enums.OperationEnum;
import org.dbsyncer.sdk.filter.CompareFilter;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Filter;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

public class Picker {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final List<Field> sourceFields = new ArrayList<>();
    private final List<Field> targetFields = new ArrayList<>();
    private final int sFieldSize;
    private final int tFieldSize;
    private final boolean enabledFilter;
    private List<Filter> add;
    private List<Filter> or;
    private SchemaResolver sourceResolver;

    public Picker(TableGroup tableGroup) {
        if (!CollectionUtils.isEmpty(tableGroup.getFieldMapping())) {
            tableGroup.getFieldMapping().forEach(m -> {
                sourceFields.add(m.getSource());
                targetFields.add(m.getTarget());
            });
        }
        this.sFieldSize = sourceFields.size();
        this.tFieldSize = targetFields.size();
        // 解析过滤条件
        List<Filter> filter = tableGroup.getFilter();
        enabledFilter = !CollectionUtils.isEmpty(filter) && filter.stream().noneMatch(f -> StringUtil.equals(f.getOperation(), OperationEnum.SQL.getName()));
        if (enabledFilter) {
            add = filter.stream().filter(f -> StringUtil.equals(f.getOperation(), OperationEnum.AND.getName())).collect(Collectors.toList());
            or = filter.stream().filter(f -> StringUtil.equals(f.getOperation(), OperationEnum.OR.getName())).collect(Collectors.toList());
        }
    }

    public List<Map> pickTargetData(List<Map> source) {
        List<Map> targetMapList = new ArrayList<>();
        if (!CollectionUtils.isEmpty(source)) {
            Map<String, Object> target = null;
            for (Map row : source) {
                target = new HashMap<>();
                exchange(sFieldSize, tFieldSize, this.sourceFields, this.targetFields, row, target);
                targetMapList.add(target);
            }
        }
        return targetMapList;
    }

    public List<Map> pickTargetData(List<Field> sourceOriginalFields, boolean enableFilter, List<List<Object>> rows, List<Map> sourceMapList) {
        List<Map> targetMapList = new ArrayList<>();
        if (CollectionUtils.isEmpty(rows)) {
            return targetMapList;
        }
        Map<String, Object> source = null;
        Map<String, Object> target = null;
        for (List<Object> row : rows) {
            // 排除下标不一致的数据
            if (row.size() != sourceOriginalFields.size()) {
                logger.warn("源表结构发生变化，与当前表字段映射关系不一致，请检查重新配置");
                continue;
            }
            source = new HashMap<>();
            for (int j = 0; j < sourceOriginalFields.size(); j++) {
                source.put(sourceOriginalFields.get(j).getName(), row.get(j));
            }
            target = new HashMap<>();
            exchange(sFieldSize, tFieldSize, this.sourceFields, this.targetFields, source, target);
            // 根据条件过滤数据
            if (enableFilter && !filter(target)) {
                continue;
            }
            sourceMapList.add(source);
            targetMapList.add(target);
        }
        return targetMapList;
    }

    public List<Object> pickSourceData(List<Field> sourceColumn, Map target) {
        Map<String, Object> source = new HashMap<>();
        if (!CollectionUtils.isEmpty(target)) {
            exchange(tFieldSize, sFieldSize, targetFields, sourceFields, target, source);
        }

        return getFields(sourceColumn).stream().map(field -> source.get(field.getName())).collect(Collectors.toList());
    }

    public List<Field> getSourceFields() {
        return getFields(sourceFields);
    }

    public List<Field> getTargetFields() {
        return getFields(targetFields);
    }

    public Map<String, Field> getTargetFieldMap() {
        return targetFields.stream().filter(Objects::nonNull).collect(Collectors.toMap(Field::getName, f -> f, (k1, k2) -> k1));
    }

    private boolean filter(Map<String, Object> row) {
        if (!enabledFilter) {
            return true;
        }
        // where (id > 1 and id < 100) or (id = 100 or id =101)
        // 或 关系(成立任意条件)
        Object value = null;
        for (Filter f : or) {
            value = row.get(f.getName());
            if (compareValueWithFilter(f, value)) {
                return true;
            }
        }

        boolean pass = false;
        // 并 关系(成立所有条件)
        for (Filter f : add) {
            value = row.get(f.getName());
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
        final FilterEnum filterEnum = FilterEnum.getFilterEnum(filter.getFilter());
        final CompareFilter compareFilter = filterEnum.getCompareFilter();

        // 支持 NULL 比较
        switch (filterEnum) {
            case IS_NULL:
            case IS_NOT_NULL:
                // 此处传入 “not null” 表示有值，没必要对原始值进行转换
                return compareFilter.compare(null == comparedValue ? null : "not null", filter.getValue());
            default:
                if (comparedValue == null) {
                    return false;
                }
                break;
        }

        // 支持时间比较
        if (comparedValue instanceof Timestamp) {
            Timestamp comparedTimestamp = (Timestamp) comparedValue;
            Timestamp filterTimestamp = DateFormatUtil.stringToTimestamp(filter.getValue());
            return compareFilter.compare(String.valueOf(comparedTimestamp.getTime()), String.valueOf(filterTimestamp.getTime()));
        }
        if (comparedValue instanceof java.sql.Date) {
            java.sql.Date comparedDate = (java.sql.Date) comparedValue;
            Date filterDate = DateFormatUtil.stringToDate(filter.getValue());
            return compareFilter.compare(String.valueOf(comparedDate.getTime()), String.valueOf(filterDate.getTime()));
        }

        return compareFilter.compare(String.valueOf(comparedValue), filter.getValue());
    }

    private void exchange(int sFieldSize, int tFieldSize, List<Field> sFields, List<Field> tFields, Map<String, Object> source, Map<String, Object> target) {
        Field sField = null;
        Field tField = null;
        Object v = null;
        String tFieldName = null;
        for (int k = 0; k < sFieldSize; k++) {
            sField = sFields.get(k);
            if (k < tFieldSize) {
                tField = tFields.get(k);
            }
            if (null != sField && null != tField) {
                v = source.get(sField.getName());
                // 合并为标准数据类型
                if (sourceResolver != null) {
                    v = sourceResolver.merge(v, sField);
                }
                tFieldName = tField.getName();
                // 映射值
                if (!target.containsKey(tFieldName)) {
                    target.put(tFieldName, v);
                    continue;
                }
                // 合并值
                target.put(tFieldName, StringUtil.toString(target.get(tFieldName)) + StringUtil.toString(v));
            }
        }
    }

    private List<Field> getFields(List<Field> list) {
        List<Field> fields = new ArrayList<>();
        Set<String> keys = new HashSet<>();
        list.forEach(f -> {
            if (f != null && !keys.contains(f.getName())) {
                fields.add(f);
                keys.add(f.getName());
            }
        });
        keys.clear();
        return Collections.unmodifiableList(fields);
    }

    public Picker setSourceResolver(SchemaResolver sourceResolver) {
        this.sourceResolver = sourceResolver;
        return this;
    }
}