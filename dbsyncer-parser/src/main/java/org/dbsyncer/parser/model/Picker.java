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

import java.sql.Date;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

public class Picker {

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

    /**
     * 从原始行数据中提取并转换为目标数据
     * <p>
     * 工作机制：
     * 1. 遍历原始行数据（List<List<Object>>），将每行数据转换为Map格式（字段名->值）
     * 2. 根据字段映射关系（sourceFields -> targetFields），通过exchange方法将源字段值映射到目标字段
     * 3. 如果启用过滤（enableFilter=true），使用filter方法根据配置的过滤条件（AND/OR）过滤数据
     * 4. 只保留通过过滤条件的数据，将源数据添加到sourceMapList，目标数据添加到返回列表
     * 5. 如果最终没有数据通过过滤，抛出异常
     *
     * @param sourceOriginalFields 源表的原始字段列表
     * @param enableFilter         是否启用过滤条件
     * @param rows                 原始数据行（每行是一个List<Object>，按字段顺序存储值）
     * @param sourceMapList        用于存储源数据的Map列表（作为输出参数，方法会向其中添加数据）
     * @return 转换后的目标数据列表（Map格式，key为目标字段名，value为字段值）
     * @throws Exception 当没有数据通过过滤时抛出异常
     */
    public List<Map> pickTargetData(List<Field> sourceOriginalFields, boolean enableFilter, List<List<Object>> rows, List<Map> sourceMapList) throws Exception {
        List<Map> targetMapList = new ArrayList<>();
        if (CollectionUtils.isEmpty(rows)) {
            return targetMapList;
        }
        Map<String, Object> source = null;
        Map<String, Object> target = null;
        for (List<Object> row : rows) {
            assert row.size() == sourceOriginalFields.size();
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
        if (targetMapList.isEmpty()) {
            throw new Exception("数据提取失败");
        }
        return targetMapList;
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