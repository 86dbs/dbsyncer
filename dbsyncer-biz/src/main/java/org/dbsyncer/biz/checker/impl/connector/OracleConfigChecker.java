package org.dbsyncer.biz.checker.impl.connector;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.biz.enums.OracleIncrementEnum;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.config.Field;
import org.dbsyncer.connector.config.MetaInfo;
import org.dbsyncer.connector.config.Table;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.model.FieldMapping;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <p>1、增量同步时，目标源必须有一个主键字段用于接收ROW_ID值。</p>
 * <p>2、全局可配置目标源ROW_ID字段名称，默认为ROW_ID_LABEL_NAME。 </p>
 * <p>3、如果配置了接收字段，添加字段映射关系[ROW_ID_LABEL_NAME] > [ROW_ID_LABEL_NAME]，并将ROW_ID_LABEL_NAME字段设置为目标源的唯一主键。</p>
 * <p>4、全量同步时，ROW_ID_LABEL_NAME参数非必须。</p>
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/8 15:17
 */
@Component
public class OracleConfigChecker extends AbstractDataBaseConfigChecker {

    @Autowired
    private Manager manager;

    @Override
    public void dealIncrementStrategy(Mapping mapping, TableGroup tableGroup, Map<String, String> params) {
        String rowIdlabelName = OracleIncrementEnum.ROW_ID_LABEL_NAME.getName();
        if (CollectionUtils.isEmpty(params) || !params.containsKey(rowIdlabelName)) {
            revert(mapping, tableGroup);
            return;
        }
        String targetRowIdName = params.get(rowIdlabelName);

        // 检查目标源是否支持该自定义字段
        List<Field> targetColumn = tableGroup.getTargetTable().getColumn();
        Field targetField = null;
        for (Field f : targetColumn) {
            if (StringUtils.equals(f.getName(), targetRowIdName)) {
                targetField = f;
                targetField.setPk(true);
                continue;
            }
            f.setPk(false);
        }
        Assert.isTrue(null != targetField,
                String.format("[%s 同步到 %s]，目标源表不存在字段%s", tableGroup.getSourceTable().getName(), tableGroup.getTargetTable().getName(),
                        targetRowIdName));

        // 检查是否更新
        for (FieldMapping m : tableGroup.getFieldMapping()) {
            if (null != m.getSource() && OracleIncrementEnum.isRowId(m.getSource().getName())) {
                m.getTarget().setName(targetRowIdName);
                return;
            }
        }

        Field sourceField = new Field(OracleIncrementEnum.ROW_ID.getName(), "VARCHAR2", 12, false,
                OracleIncrementEnum.ROW_ID_LABEL_NAME.getName(), true);
        tableGroup.getSourceTable().getColumn().add(0, sourceField);

        // 取消主键
        tableGroup.getFieldMapping().forEach(m -> {
            if (null != m.getTarget()) {
                m.getTarget().setPk(false);
            }
        });
        tableGroup.getFieldMapping().add(0, new FieldMapping(sourceField, targetField));
    }

    /**
     * 还原字段和映射关系
     *
     * @param mapping
     * @param tableGroup
     */
    private void revert(Mapping mapping, TableGroup tableGroup) {
        List<FieldMapping> fieldMapping = tableGroup.getFieldMapping();
        if (CollectionUtils.isEmpty(fieldMapping)) {
            return;
        }

        // 还原字段
        Table sourceTable = tableGroup.getSourceTable();
        List<Field> sourceColumn = sourceTable.getColumn();
        List<Field> sourceFields = new ArrayList<>();
        boolean existRowId = false;
        for (Field f : sourceColumn) {
            if (OracleIncrementEnum.isRowId(f.getName())) {
                existRowId = true;
                continue;
            }
            sourceFields.add(f);
        }
        sourceTable.setColumn(sourceFields);

        if (!existRowId) {
            return;
        }

        // 存在自定义主键
        String pk = null;
        for (FieldMapping m : tableGroup.getFieldMapping()) {
            if (null != m.getSource() && OracleIncrementEnum.isRowId(m.getSource().getName())) {
                continue;
            }
            if (null != m.getTarget() && m.getTarget().isPk()) {
                pk = m.getTarget().getName();
                break;
            }
        }

        // 没有自定义主键，获取表元信息
        if (null == pk) {
            Table targetTable = tableGroup.getTargetTable();
            MetaInfo metaInfo = manager.getMetaInfo(mapping.getTargetConnectorId(), targetTable.getName());
            List<Field> targetColumn = metaInfo.getColumn();
            targetTable.setColumn(targetColumn);

            if (!CollectionUtils.isEmpty(targetColumn)) {
                for (Field f : targetColumn) {
                    if (f.isPk()) {
                        pk = f.getName();
                        break;
                    }
                }
            }
        }

        // 剔除映射关系
        List<FieldMapping> list = new ArrayList<>();
        for (FieldMapping m : tableGroup.getFieldMapping()) {
            if (null != m.getSource() && OracleIncrementEnum.isRowId(m.getSource().getName())) {
                continue;
            }

            if (null != m.getTarget() && StringUtils.equals(m.getTarget().getName(), pk)) {
                m.getTarget().setPk(true);
            }
            list.add(m);
        }
        fieldMapping.clear();
        fieldMapping.addAll(list);
    }

}